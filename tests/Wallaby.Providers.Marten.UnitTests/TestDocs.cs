namespace Wallaby.Providers.Marten.UnitTests;

// Top-level types: Marten derives table aliases from the type name, and nesting would prefix them
// (mt_doc_outer_plaindoc).

public class PlainDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

public class SoftDoc
{
    public Guid Id { get; set; }
    public string Name { get; set; } = "";
}

public class TenantDoc
{
    public string Id { get; set; } = "";
    public string Name { get; set; } = "";
}

public class BaseDoc
{
    public Guid Id { get; set; }
}

public class SubDoc : BaseDoc;

public class Unregistered
{
    public Guid Id { get; set; }
}
