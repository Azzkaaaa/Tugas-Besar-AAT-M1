use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use http_body::{Body as HttpBody, Frame, SizeHint};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::{Status, body::Body};
use tower::{Layer, Service};

/// Full subscriptions share one payload pipeline per feed replica. SUMMARY
/// subscriptions do not consume this credit. Acquire after authorization and
/// before replay can load the first full body.
#[derive(Clone)]
pub struct FullStreams(Arc<Semaphore>);

impl Default for FullStreams {
    fn default() -> Self {
        Self(Arc::new(Semaphore::new(1)))
    }
}

impl FullStreams {
    pub fn acquire(&self) -> Result<FullStreamCredit, Status> {
        self.0
            .clone()
            .try_acquire_owned()
            .map(|permit| FullStreamCredit { _permit: Arc::new(permit) })
            .map_err(|_| Status::resource_exhausted("full feed capacity is busy; retry later"))
    }
}

#[derive(Clone)]
pub struct FullStreamCredit {
    _permit: Arc<OwnedSemaphorePermit>,
}

/// Tonic carries the typed response extension to its HTTP response. Transfer
/// it into the body and every data frame, so queued/split h2 Bytes still own
/// credit even after the producer stream/body has been dropped.
#[derive(Clone, Copy)]
pub struct StreamAdmissionLayer;

impl<S> Layer<S> for StreamAdmissionLayer {
    type Service = StreamAdmissionService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        StreamAdmissionService { inner }
    }
}

#[derive(Clone)]
pub struct StreamAdmissionService<S> {
    inner: S,
}

impl<S, B> Service<http::Request<B>> for StreamAdmissionService<S>
where
    S: Service<http::Request<B>, Response = http::Response<Body>>,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let response = self.inner.call(request);
        Box::pin(async move {
            let mut response = response.await?;
            if let Some(credit) = response.extensions_mut().remove::<FullStreamCredit>() {
                let (parts, body) = response.into_parts();
                response = http::Response::from_parts(parts, Body::new(HeldBody { body, credit }));
            }
            Ok(response)
        })
    }
}

struct HeldBody {
    body: Body,
    credit: FullStreamCredit,
}

impl HttpBody for HeldBody {
    type Data = Bytes;
    type Error = Status;
    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let credit = self.credit.clone();
        Pin::new(&mut self.body).poll_frame(cx).map(|frame| {
            frame.map(|frame| frame.map(|frame| frame.map_data(|data| Bytes::from_owner(HeldBytes { data, _credit: credit }))))
        })
    }
    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }
}

struct HeldBytes {
    data: Bytes,
    _credit: FullStreamCredit,
}
impl AsRef<[u8]> for HeldBytes {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::Infallible;
    use std::future::{Ready, poll_fn, ready};

    struct OneFrame(Option<Bytes>);
    impl HttpBody for OneFrame {
        type Data = Bytes;
        type Error = Status;
        fn poll_frame(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
            Poll::Ready(self.0.take().map(|data| Ok(Frame::data(data))))
        }
    }
    struct ResponseService {
        credit: FullStreamCredit,
        data: Bytes,
    }
    impl Service<http::Request<()>> for ResponseService {
        type Response = http::Response<Body>;
        type Error = Infallible;
        type Future = Ready<Result<Self::Response, Self::Error>>;
        fn poll_ready(&mut self, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, _: http::Request<()>) -> Self::Future {
            let mut response = http::Response::new(Body::new(OneFrame(Some(self.data.clone()))));
            response.extensions_mut().insert(self.credit.clone());
            ready(Ok(response))
        }
    }

    #[tokio::test]
    async fn queued_and_split_bytes_hold_credit_after_response_and_body_drop() {
        let admission = FullStreams::default();
        let original = Bytes::from(vec![42; 2 << 20]);
        let mut service = StreamAdmissionLayer.layer(ResponseService { credit: admission.acquire().unwrap(), data: original.clone() });
        let mut response = service.call(http::Request::new(())).await.unwrap();
        drop(service);
        assert!(response.extensions().get::<FullStreamCredit>().is_none());
        let bytes = poll_fn(|cx| Pin::new(response.body_mut()).poll_frame(cx)).await.unwrap().unwrap().into_data().unwrap();
        assert_eq!(bytes.as_ptr(), original.as_ptr(), "transport admission copied the payload");
        let last_slice = bytes.slice(100..1000);
        drop(response);
        drop(bytes);
        assert_eq!(admission.acquire().err().unwrap().code(), tonic::Code::ResourceExhausted);
        drop(last_slice);
        assert!(admission.acquire().is_ok());
    }
}
