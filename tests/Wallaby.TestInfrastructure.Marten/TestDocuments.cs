namespace Wallaby.TestInfrastructure.Marten;

/// <summary>Plain Guid-id document (single tenancy, hard deletes).</summary>
public class Widget
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public int Qty { get; set; }
}

/// <summary>
/// Soft-deleted document: deletes flip <c>mt_deleted</c> instead of removing the row, and the large
/// <see cref="Payload"/> makes <c>data</c> TOAST so undeletes exercise the REPLICA IDENTITY FULL fallback.
/// </summary>
public class SoftWidget
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
    public string Payload { get; set; } = "";
}

/// <summary>Conjoined-tenancy document with a string id (the key is [tenant_id, id]).</summary>
public class TenantWidget
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}
