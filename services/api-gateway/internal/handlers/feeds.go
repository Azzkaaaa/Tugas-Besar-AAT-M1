package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/common/v1"
	feedv1 "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-aggregator-go/bnpb/feed/v1"
	gw "github.com/Azzkaaaa/Tugas-Besar-AAT-M1/packages/bnpb-gateway-go/gatewayapi"

	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/exchange"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/mapping"
	"github.com/Azzkaaaa/Tugas-Besar-AAT-M1/services/api-gateway/internal/problem"
)

func (h *Handlers) StreamHazardEvents(ctx context.Context, request gw.StreamHazardEventsRequestObject) (gw.StreamHazardEventsResponseObject, error) {
	p := request.Params
	view, err := mapping.View(p.View)
	if err != nil {
		return nil, err
	}
	filter, err := mapping.Filter(p.Source, p.HazardType, p.Severity, nil, nil)
	if err != nil {
		return nil, err
	}
	body, err := h.stream(ctx, filter, view, p.LastEventID)
	if err != nil {
		return nil, err
	}
	return gw.StreamHazardEvents200TexteventStreamResponse{EventStreamTexteventStreamResponse: body}, nil
}

func (h *Handlers) StreamSeismicHazards(ctx context.Context, request gw.StreamSeismicHazardsRequestObject) (gw.StreamSeismicHazardsResponseObject, error) {
	view, err := mapping.View(request.Params.View)
	if err != nil {
		return nil, err
	}
	body, err := h.alias(ctx, commonv1.HazardType_HAZARD_TYPE_SEISMIC, view, request.Params.LastEventID)
	if err != nil {
		return nil, err
	}
	return gw.StreamSeismicHazards200TexteventStreamResponse{EventStreamTexteventStreamResponse: body}, nil
}

func (h *Handlers) StreamVolcanicHazards(ctx context.Context, request gw.StreamVolcanicHazardsRequestObject) (gw.StreamVolcanicHazardsResponseObject, error) {
	view, err := mapping.View(request.Params.View)
	if err != nil {
		return nil, err
	}
	body, err := h.alias(ctx, commonv1.HazardType_HAZARD_TYPE_VOLCANIC, view, request.Params.LastEventID)
	if err != nil {
		return nil, err
	}
	return gw.StreamVolcanicHazards200TexteventStreamResponse{EventStreamTexteventStreamResponse: body}, nil
}

// alias feeds fix hazard_type and declare no filter parameters, so passing one is an error
// rather than a silently ignored filter.
func (h *Handlers) alias(ctx context.Context, hazardType commonv1.HazardType, view commonv1.View, lastEventID *string) (gw.EventStreamTexteventStreamResponse, error) {
	if request := exchange.From(ctx).Request; request != nil {
		query := request.URL.Query()
		for _, name := range []string{"source", "hazard_type", "severity"} {
			if query.Has(name) {
				return gw.EventStreamTexteventStreamResponse{}, problem.BadRequest("%s is not supported on this feed", name)
			}
		}
	}
	return h.stream(ctx, &commonv1.HazardFilter{HazardType: hazardType}, view, lastEventID)
}

// stream opens SubscribeEvents and returns the SSE body. Rejections that arrive before the
// stream opens become problem responses; an expired or lost replay position still opens the
// stream, emits reset_required and closes (per contract).
func (h *Handlers) stream(ctx context.Context, filter *commonv1.HazardFilter, view commonv1.View, lastEventID *string) (gw.EventStreamTexteventStreamResponse, error) {
	var none gw.EventStreamTexteventStreamResponse
	request := &feedv1.SubscribeEventsRequest{View: view, Filter: filter, ResumeAfter: lastEventID}
	correlationID := exchange.From(ctx).CorrelationID
	// The body outlives this call: the stream context is cancelled by the pump, or by the
	// request context when the client goes away.
	streamCtx, cancel := context.WithCancel(ctx)
	subscription, err := h.Clients.Feed.SubscribeEvents(streamCtx, request)
	if err == nil {
		// Header blocks until the feed accepts or rejects. No headers means a trailers-only
		// rejection whose status Recv reports.
		var header map[string][]string
		if header, err = subscription.Header(); err == nil && header == nil {
			if _, err = subscription.Recv(); err == nil {
				err = status.Error(codes.Internal, "feed sent a message without headers")
			}
		}
	}
	if err != nil {
		cancel()
		if status.Code(err) == codes.OutOfRange {
			frame, frameErr := resetFrame(correlationID)
			if frameErr != nil {
				return none, frameErr
			}
			return eventStream(strings.NewReader(frame)), nil
		}
		return none, upstream(err)
	}

	reader, writer := io.Pipe()
	go func() {
		defer cancel()
		_ = writer.CloseWithError(pump(streamCtx, subscription, writer, view, correlationID, h.Heartbeat))
	}()
	return eventStream(reader), nil
}

func eventStream(body io.Reader) gw.EventStreamTexteventStreamResponse {
	cacheControl := "no-cache, no-transform"
	return gw.EventStreamTexteventStreamResponse{Body: body, Headers: gw.EventStreamResponseHeaders{CacheControl: &cacheControl}}
}

// pump forwards feed events as SSE frames with heartbeat comments until the feed or the
// client ends the stream. Its error closes the pipe; nil means a clean end.
func pump(ctx context.Context, subscription feedv1.EventFeedService_SubscribeEventsClient, out io.Writer,
	view commonv1.View, correlationID string, heartbeat time.Duration,
) error {
	type received struct {
		response *feedv1.SubscribeEventsResponse
		err      error
	}
	events := make(chan received)
	go func() {
		for {
			response, err := subscription.Recv()
			select {
			case events <- received{response, err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()
	ticker := time.NewTicker(heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if _, err := io.WriteString(out, ": keepalive\n\n"); err != nil {
				return err
			}
		case next := <-events:
			if next.err != nil {
				switch code := status.Code(next.err); {
				case code == codes.OutOfRange:
					frame, err := resetFrame(correlationID)
					if err == nil {
						_, err = io.WriteString(out, frame)
					}
					return err
				case errors.Is(next.err, io.EOF), code == codes.Canceled:
					return nil
				default:
					// Expiry/revocation (UNAUTHENTICATED) and failures close the stream; the
					// HTTP status is already sent.
					slog.InfoContext(ctx, "feed stream closed", "code", code.String())
					return nil
				}
			}
			frame, err := hazardFrame(next.response.GetEvent(), view)
			if err != nil {
				slog.ErrorContext(ctx, "feed event cannot be rendered; closing stream", "error", err)
				return nil
			}
			if _, err := io.WriteString(out, frame); err != nil {
				return err
			}
		}
	}
}

func hazardFrame(event *feedv1.FeedEvent, view commonv1.View) (string, error) {
	if event.GetEventId() == "" || event.GetSequence() == 0 {
		return "", errors.New("feed event without event_id or sequence")
	}
	cursor := event.GetResumeCursor()
	if cursor == "" || strings.ContainsAny(cursor, "\r\n") {
		return "", errors.New("feed event without a usable resume_cursor")
	}
	hazard, err := mapping.Hazard(event.GetHazard(), view)
	if err != nil {
		return "", err
	}
	publishedAt, err := mapping.RequiredTime("published_at", event.GetPublishedAt())
	if err != nil {
		return "", err
	}
	data, err := json.Marshal(gw.HazardFeedEvent{
		EventId:     event.GetEventId(),
		Sequence:    strconv.FormatUint(event.GetSequence(), 10),
		Hazard:      hazard,
		PublishedAt: publishedAt,
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("id: %s\nevent: hazard.upsert\ndata: %s\n\n", cursor, data), nil
}

// resetFrame has no id line: there is no position to resume from.
func resetFrame(correlationID string) (string, error) {
	data, err := json.Marshal(gw.ResetRequiredEvent{Reason: gw.REPLAYWINDOWEXPIRED, CorrelationId: correlationID})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("event: reset_required\ndata: %s\n\n", data), nil
}
