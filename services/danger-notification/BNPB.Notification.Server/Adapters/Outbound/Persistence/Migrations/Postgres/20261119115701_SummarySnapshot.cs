using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace BNPB.Notification.Server.Adapters.Outbound.Persistence.Migrations.Postgres
{
    /// <inheritdoc />
    public partial class SummarySnapshot : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AddColumn<byte[]>(
                name: "Summary",
                table: "notifications",
                type: "bytea",
                nullable: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropColumn(
                name: "Summary",
                table: "notifications");
        }
    }
}
