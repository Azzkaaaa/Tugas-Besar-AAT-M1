using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence.Migrations.Postgres
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "notifications",
                columns: table => new
                {
                    Id = table.Column<string>(type: "text", nullable: false),
                    HazardId = table.Column<string>(type: "text", nullable: false),
                    Source = table.Column<int>(type: "integer", nullable: false),
                    HazardType = table.Column<int>(type: "integer", nullable: false),
                    Severity = table.Column<int>(type: "integer", nullable: false),
                    AreaName = table.Column<string>(type: "text", nullable: false),
                    OccurredAt = table.Column<long>(type: "bigint", nullable: false),
                    PublishedAt = table.Column<long>(type: "bigint", nullable: false),
                    CreatedAt = table.Column<long>(type: "bigint", nullable: false),
                    Hazard = table.Column<byte[]>(type: "bytea", nullable: false),
                    IsCurrent = table.Column<bool>(type: "boolean", nullable: false),
                    PushStatus = table.Column<int>(type: "integer", nullable: false),
                    FailedPushAttempts = table.Column<int>(type: "integer", nullable: false),
                    NextPushAttemptAt = table.Column<long>(type: "bigint", nullable: true),
                    LeaseToken = table.Column<Guid>(type: "uuid", nullable: true),
                    LeaseExpiresAt = table.Column<long>(type: "bigint", nullable: true),
                    TraceParent = table.Column<string>(type: "text", nullable: true),
                    xmin = table.Column<uint>(type: "xid", rowVersion: true, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_notifications", x => x.Id);
                });

            migrationBuilder.CreateIndex(
                name: "IX_notifications_HazardId",
                table: "notifications",
                column: "HazardId",
                unique: true,
                filter: "\"IsCurrent\"");

            migrationBuilder.CreateIndex(
                name: "IX_notifications_HazardId_IsCurrent",
                table: "notifications",
                columns: new[] { "HazardId", "IsCurrent" });

            migrationBuilder.CreateIndex(
                name: "IX_notifications_IsCurrent_CreatedAt_Id",
                table: "notifications",
                columns: new[] { "IsCurrent", "CreatedAt", "Id" },
                descending: new[] { false, true, false });

            migrationBuilder.CreateIndex(
                name: "IX_notifications_PushStatus_NextPushAttemptAt",
                table: "notifications",
                columns: new[] { "PushStatus", "NextPushAttemptAt" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "notifications");
        }
    }
}
