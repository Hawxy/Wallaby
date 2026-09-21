using System.ComponentModel.DataAnnotations.Schema;

namespace Wallaby.AotSmokeTest;

/// <summary>A positional record for the plain-table provider check.</summary>
[Table("smoke_rows", Schema = "smoke")]
public sealed record SmokeRow(Guid Id, string Name, int Qty);
