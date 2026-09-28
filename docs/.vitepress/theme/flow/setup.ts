// Options for the getting-started picker and the setup code it generates:
// the packages to install and a Program.cs registration for the chosen
// provider, sinks and external slot.

export interface ProviderOption {
  id: string;
  title: string;
  sub: string;
  link: string;
  pkg: string;
  // registered before AddWallaby
  prelude: string[];
  use: string;
}

export interface SinkOption {
  id: string;
  title: string;
  sub: string;
  link: string;
  pkg?: string; // custom sinks need none
  add: string;
  // the options lambda body, if the sink takes one
  body?: string[];
}

export const providers: ProviderOption[] = [
  {
    id: 'ef',
    title: 'ef core',
    sub: 'entity model',
    link: '/providers/entity-framework-core',
    pkg: 'Wallaby.Providers.EntityFrameworkCore',
    prelude: ['builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(conn));'],
    use: 'UseEntityFrameworkCore<AppDbContext>()',
  },
  {
    id: 'marten',
    title: 'marten',
    sub: 'documents',
    link: '/providers/marten',
    pkg: 'Wallaby.Providers.Marten',
    prelude: [
      'builder.Services.AddMarten(o =>',
      '{',
      '    o.Connection(conn);',
      '    o.RegisterDocumentType<Product>();',
      '});',
    ],
    use: 'UseMarten()',
  },
  {
    id: 'tables',
    title: 'tables',
    sub: 'plain pocos',
    link: '/providers/tables',
    pkg: 'Wallaby.Providers.Tables',
    prelude: [],
    use: 'UseTables(tables => tables.Add<Product>())',
  },
];

export const sinks: SinkOption[] = [
  {
    id: 'meilisearch',
    title: 'meilisearch',
    sub: 'search index',
    link: '/sinks/meilisearch',
    pkg: 'Wallaby.Sinks.Meilisearch',
    add: 'AddMeilisearchSink("meili", m =>',
    body: ['m.Endpoint = "http://localhost:7700";', 'm.ApiKey = meiliKey;'],
  },
  {
    id: 'http',
    title: 'http',
    sub: 'POST to any endpoint',
    link: '/sinks/http',
    pkg: 'Wallaby.Sinks.Http',
    add: 'AddHttpSink("webhook", o =>',
    body: ['o.Endpoint = "https://api.example.com/wallaby";', 'o.SigningSecret = signingSecret;'],
  },
  {
    id: 'kafka',
    title: 'kafka',
    sub: 'event streaming',
    link: '/sinks/kafka',
    pkg: 'Wallaby.Sinks.Kafka',
    add: 'AddKafkaSink("kafka", k =>',
    body: ['k.BootstrapServers = "localhost:9092";'],
  },
  {
    id: 'elasticsearch',
    title: 'elasticsearch',
    sub: 'search + analytics',
    link: '/sinks/elasticsearch',
    pkg: 'Wallaby.Sinks.Elasticsearch',
    add: 'AddElasticsearchSink("elastic", s =>',
    body: ['s.Endpoint = "https://localhost:9200";', 's.ApiKey = elasticKey;'],
  },
  {
    id: 'opensearch',
    title: 'opensearch',
    sub: 'search + analytics',
    link: '/sinks/opensearch',
    pkg: 'Wallaby.Sinks.OpenSearch',
    add: 'AddOpenSearchSink("opensearch", s =>',
    body: ['s.Endpoint = "https://localhost:9200";', 's.Username = "wallaby";', 's.Password = openSearchPassword;'],
  },
  {
    id: 'pgvector',
    title: 'pgvector',
    sub: 'vectors in postgres',
    link: '/sinks/pgvector',
    pkg: 'Wallaby.Sinks.Pgvector',
    add: 'AddPgvectorSink("vectors", v =>',
    body: [
      'v.ConnectionString = vectorConn;',
      'v.Dimensions = 1536;',
      'v.EmbeddingGenerator = embeddingGenerator;',
      'v.EmbedText = d => (string?)d["description"];',
      'v.EmbeddingVersion = "text-embedding-3-small/1";',
    ],
  },
  {
    id: 'custom',
    title: 'custom',
    sub: 'your own target',
    link: '/sinks/custom',
    add: 'AddSink("my-sink", sp => new MySink())',
  },
];

export interface Choice {
  provider: ProviderOption;
  sinks: SinkOption[];
  externalSlot: boolean;
}

function packages({ provider, sinks }: Choice) {
  return [provider.pkg, ...sinks.flatMap(s => (s.pkg ? [s.pkg] : []))];
}

export function installCommands(choice: Choice) {
  return packages(choice)
    .map(pkg => `dotnet add package ${pkg}`)
    .join('\n');
}

export function programCs(choice: Choice) {
  const { provider, sinks, externalSlot } = choice;
  const chain = [`cdc.${provider.use}`, '   .UseConnectionString(conn)'];
  for (const s of sinks) {
    chain.push(`   .${s.add}`);
    if (s.body) chain.push('   {', ...s.body.map(l => `       ${l}`), '   })');
    chain.push(
      '   .WithMappings(sink => sink',
      '       .Map<Product>()',
      '       .ToDestination("products")',
      '       .UsingTransform(/* ... */))',
    );
  }
  if (externalSlot) {
    chain.push(
      '',
      '   // for an external pgoutput consumer; wallaby never reads it',
      '   .AddExternalSlot("elt", s => s.ForAllEntities())',
    );
  }
  chain[chain.length - 1] += ';';

  const usings = ['Wallaby.DependencyInjection', ...packages(choice)];
  return [
    ...usings.map(ns => `using ${ns};`),
    '',
    'var builder = Host.CreateApplicationBuilder(args);',
    '',
    ...(provider.prelude.length ? [...provider.prelude, ''] : []),
    'builder.Services.AddWallaby(cdc =>',
    '{',
    ...chain.map(l => (l ? `    ${l}` : '')),
    '});',
    '',
    'await builder.Build().RunAsync();',
  ].join('\n');
}
