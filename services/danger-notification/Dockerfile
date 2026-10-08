# Build from the repository root:
#   docker compose build notification-service
#
# The proto codegen/ folders under packages/bnpb-*-dotnet are gitignored, so
# generate them first: npx nx run-many -t generate-proto

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS builder
WORKDIR /src

# Restore against project files only so the layer survives source changes.
COPY packages/bnpb-common-dotnet/BNPB.Common/BNPB.Common.csproj packages/bnpb-common-dotnet/BNPB.Common/
COPY packages/bnpb-event-dotnet/BNPB.Event/BNPB.Event.csproj packages/bnpb-event-dotnet/BNPB.Event/
COPY packages/bnpb-aggregator-dotnet/BNPB.Aggregator/BNPB.Aggregator.csproj packages/bnpb-aggregator-dotnet/BNPB.Aggregator/
COPY packages/bnpb-notification-dotnet/BNPB.Notification/BNPB.Notification.csproj packages/bnpb-notification-dotnet/BNPB.Notification/
COPY services/danger-notification/BNPB.Notification.Server/BNPB.Notification.Server.csproj services/danger-notification/BNPB.Notification.Server/
RUN dotnet restore services/danger-notification/BNPB.Notification.Server/BNPB.Notification.Server.csproj

COPY packages/bnpb-common-dotnet packages/bnpb-common-dotnet
COPY packages/bnpb-event-dotnet packages/bnpb-event-dotnet
COPY packages/bnpb-aggregator-dotnet packages/bnpb-aggregator-dotnet
COPY packages/bnpb-notification-dotnet packages/bnpb-notification-dotnet
COPY services/danger-notification/BNPB.Notification.Server services/danger-notification/BNPB.Notification.Server
# No --no-restore: the legacy builder ignores Dockerfile.dockerignore, so host
# obj/ folders can overwrite the restore output above. Restoring again is a
# cheap no-op when they don't.
RUN dotnet publish services/danger-notification/BNPB.Notification.Server/BNPB.Notification.Server.csproj \
    -c Release -o /app

###
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app

COPY --from=builder /app .
USER app

EXPOSE 9095

ENTRYPOINT ["dotnet", "BNPB.Notification.Server.dll"]
