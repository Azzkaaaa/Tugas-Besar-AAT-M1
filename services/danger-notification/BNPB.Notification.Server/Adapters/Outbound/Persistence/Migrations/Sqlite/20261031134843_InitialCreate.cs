using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence.Migrations.Sqlite
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
                    Id = table.Column<string>(type: "TEXT", nullable: false),
                    HazardId = table.Column<string>(type: "TEXT", nullable: false),
                    Source = table.Column<int>(type: "INTEGER", nullable: false),
                    HazardType = table.Column<int>(type: "INTEGER", nullable: false),
                    Severity = table.Column<int>(type: "INTEGER", nullable: false),
                    AreaName = table.Column<string>(type: "TEXT", nullable: false),
                    OccurredAt = table.Column<long>(type: "INTEGER", nullable: false),
                    PublishedAt = table.Column<long>(type: "INTEGER", nullable: false),
                    CreatedAt = table.Column<long>(type: "INTEGER", nullable: false),
                    Hazard = table.Column<byte[]>(type: "BLOB", nullable: false),
                    IsCurrent = table.Column<bool>(type: "INTEGER", nullable: false),
                    PushStatus = table.Column<int>(type: "INTEGER", nullable: false),
                    FailedPushAttempts = table.Column<int>(type: "INTEGER", nullable: false),
                    NextPushAttemptAt = table.Column<long>(type: "INTEGER", nullable: true),
                    LeaseToken = table.Column<Guid>(type: "TEXT", nullable: true),
                    LeaseExpiresAt = table.Column<long>(type: "INTEGER", nullable: true),
                    TraceParent = table.Column<string>(type: "TEXT", nullable: true)
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
                filter: "\"IsCurrent\" = 1");

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
