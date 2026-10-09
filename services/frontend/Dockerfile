FROM node:22-alpine AS build

WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
ARG VITE_APP_MODE=oidc
ARG VITE_ENABLE_DEMO=false
ARG VITE_DATA_MODE=gateway
ARG VITE_API_BASE_URL=/api/v1
# Empty Firebase values build without push.
ARG VITE_FIREBASE_API_KEY=
ARG VITE_FIREBASE_PROJECT_ID=
ARG VITE_FIREBASE_MESSAGING_SENDER_ID=
ARG VITE_FIREBASE_APP_ID=
ARG VITE_FIREBASE_VAPID_KEY=
ENV VITE_APP_MODE=${VITE_APP_MODE} \
    VITE_ENABLE_DEMO=${VITE_ENABLE_DEMO} \
    VITE_DATA_MODE=${VITE_DATA_MODE} \
    VITE_API_BASE_URL=${VITE_API_BASE_URL} \
    VITE_FIREBASE_API_KEY=${VITE_FIREBASE_API_KEY} \
    VITE_FIREBASE_PROJECT_ID=${VITE_FIREBASE_PROJECT_ID} \
    VITE_FIREBASE_MESSAGING_SENDER_ID=${VITE_FIREBASE_MESSAGING_SENDER_ID} \
    VITE_FIREBASE_APP_ID=${VITE_FIREBASE_APP_ID} \
    VITE_FIREBASE_VAPID_KEY=${VITE_FIREBASE_VAPID_KEY}
RUN npm run build

# The -otel variant ships ngx_otel_module; load_module must sit in the main nginx.conf.
FROM nginx:1.31.6-alpine-otel AS runtime
RUN sed -i '1i load_module modules/ngx_otel_module.so;' /etc/nginx/nginx.conf
ENV NGINX_OTEL_TRACE=off \
    NGINX_OTEL_ENDPOINT=localhost:4317
COPY otel.conf.template /etc/nginx/templates/otel.conf.template
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html

EXPOSE 80
HEALTHCHECK --interval=15s --timeout=3s --start-period=5s --retries=3 \
    CMD wget -q -O /dev/null http://127.0.0.1/health || exit 1
