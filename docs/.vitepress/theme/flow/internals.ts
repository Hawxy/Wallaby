// Data for the "How It Works" internals diagram: node/edge geometry on a
// fixed 704x856 canvas, group boxes, and the scenario walkthroughs that
// animate over it. Kept out of the component so the layout reads as data.

export const CANVAS = { w: 704, h: 856 };

export type IntNode = {
  id: string;
  x: number;
  y: number;
  w: number;
  h: number;
  title: string;
  subs: string[];
  detail: string;
  links: { text: string; href: string }[];
};

export type IntEdge = {
  id: string;
  points: [number, number][];
  dashed?: boolean;
  label?: string;
  lx?: number;
  ly?: number;
  vertical?: boolean;
};

export type IntGroup = {
  id: string;
  label: string;
  x: number;
  y: number;
  w: number;
  h: number;
};

export type IntStep = {
  caption: string;
  nodes?: string[];
  edges?: string[];
  /** packet + highlight color flips to blue - snapshot data, not a live WAL change */
  blue?: boolean;
  /** nodes drawn in the dashed "something is wrong" state */
  warn?: string[];
  fx?: 'tick' | 'deliver' | 'flush';
};

export type IntScenario = {
  id: string;
  label: string;
  blurb: string;
  steps: IntStep[];
};

export const nodes: IntNode[] = [
  {
    id: 'tables', x: 26, y: 44, w: 196, h: 59,
    title: 'tables',
    subs: ['app ▸ migration ▸ bulk'],
    detail: 'Your ordinary schema - nothing to install in it. Only entities you declare in a mapping are captured, and every writer is seen: the application, migrations, bulk updates, admin scripts, other services sharing the database.',
    links: [{ text: 'mappings', href: '/mappings' }],
  },
  {
    id: 'wal', x: 26, y: 127, w: 196, h: 59,
    title: 'write-ahead log',
    subs: [],
    detail: 'The write-ahead log is the source of truth: every committed row change is already in it, in commit order. Reading changes from the WAL is what removes dual writes, outbox tables, and missed rows.',
    links: [],
  },
  {
    id: 'pub', x: 26, y: 210, w: 196, h: 77,
    title: 'publication',
    subs: ['captured tables only', 'column lists'],
    detail: 'Wallaby self-configures a publication covering exactly the captured tables and reconciles it on startup. Each table\'s column list is narrowed to the columns your mappings consume, so unread values never leave the server.',
    links: [{ text: 'publication column lists', href: '/configuration#publication-column-lists' }],
  },
  {
    id: 'slot', x: 26, y: 311, w: 196, h: 77,
    title: 'replication slot',
    subs: ['pgoutput, commit order'],
    detail: 'A logical replication slot decodes the WAL through pgoutput and retains it until Wallaby confirms delivery, so nothing is lost while the consumer is away. Wallaby provisions the slot itself, or attaches to an external one you manage.',
    links: [{ text: 'external slots', href: '/external-slots' }],
  },
  {
    id: 'stream', x: 266, y: 44, w: 196, h: 59,
    title: 'replication stream',
    subs: ['keepalives ▸ flush pos'],
    detail: 'The elected leader (a Postgres advisory lock - only one node streams) opens the replication connection, answers keepalives, and reports its flushed position, which is what lets the server release retained WAL.',
    links: [{ text: 'health checks', href: '/operations/health-checks' }],
  },
  {
    id: 'assemble', x: 266, y: 127, w: 196, h: 77,
    title: 'decode + assemble',
    subs: ['committed tx in order', 'large tx spill'],
    detail: 'pgoutput messages are assembled into whole transactions that only surface on commit - rolled-back work is never seen. A streamed transaction too large to hold in memory spills to an unlogged table in the wallaby schema (or to disk, configurable) and streams back out at commit.',
    links: [],
  },
  {
    id: 'materialize', x: 266, y: 228, w: 196, h: 59,
    title: 'materialize',
    subs: ['rows ▸ entity types'],
    detail: 'The storage provider maps each raw row change back to the type you already have - an EF Core entity or a Marten document - before transforms run.',
    links: [{ text: 'storage providers', href: '/providers/overview' }],
  },
  {
    id: 'route', x: 266, y: 311, w: 196, h: 59,
    title: 'route mappings',
    subs: ['group by entity ▸ sink'],
    detail: 'Changes group by entity mapping and route to every sink that declared them. A change on a dependent table diverts here into fan-out, which re-emits the entities that reference it.',
    links: [{ text: 'mappings', href: '/mappings' }],
  },
  {
    id: 'transform', x: 266, y: 394, w: 196, h: 77,
    title: 'transform + batch',
    subs: ['enrich ▸ shape docs', 'slice ≤ maxbatchsize'],
    detail: 'Your transform shapes each change into the output document, with a leased DbContext/session for enrichment lookups. Results are sliced into batches of at most MaxBatchSize before delivery.',
    links: [
      { text: 'transforms', href: '/mappings#transforms' },
      { text: 'maxbatchsize', href: '/configuration#general-options' },
    ],
  },
  {
    id: 'dispatch', x: 266, y: 495, w: 196, h: 59,
    title: 'sink dispatcher',
    subs: ['parallel ▸ retries'],
    detail: 'Independent sinks are written concurrently. A failing batch retries with backoff, and if retries exhaust the pipeline halts rather than skipping data - a gap is never delivered around.',
    links: [{ text: 'custom sinks', href: '/sinks/custom' }],
  },
  {
    id: 'backfill', x: 496, y: 127, w: 188, h: 77,
    title: 'backfill',
    subs: ['keyset ▸ watermarks', 'feeds same pipeline'],
    detail: 'Initial snapshots, version-triggered reindexes, fan-out tails, and gap repair all run here: a keyset pager walks each table in primary-key order while watermarks fence off races with the live stream. Rows feed the same transform and sink path as live changes.',
    links: [{ text: 'backfill', href: '/backfill' }],
  },
  {
    id: 'fanout', x: 496, y: 311, w: 188, h: 77,
    title: 'fan-out',
    subs: ['dependent tables', 'consolidated lookups'],
    detail: 'When a dependent table changes, every affected parent key is resolved with one consolidated IN (…) query per relationship. The first page re-emits inline as synthetic updates; anything beyond it is offloaded so a million-row fan-out cannot stall replication.',
    links: [{ text: 'dependent tables', href: '/providers/entity-framework-core/#dependent-tables' }],
  },
  {
    id: 'queue', x: 496, y: 452, w: 188, h: 77,
    title: 'fan-out queue',
    subs: ['offloaded tail jobs', 'listen / notify'],
    detail: 'The offloaded fan-out tail lands in a queue table inside the wallaby schema. LISTEN/NOTIFY wakes the drain worker the instant a job is enqueued; the poll interval is only a safety net. Repeat changes to the same principal coalesce into one pending job.',
    links: [
      { text: 'how fan-out scales', href: '/providers/entity-framework-core/#how-fan-out-scales' },
      { text: 'fanoutpollinterval', href: '/configuration#advanced-options' },
    ],
  },
  {
    id: 'checkpoint', x: 496, y: 553, w: 188, h: 59,
    title: 'wallaby.checkpoint',
    subs: [],
    detail: 'A single row holding the last applied LSN, saved alongside acknowledgements (throttled to one write per CheckpointSaveInterval). If a recreated slot\'s consistent point is ever ahead of it, changes were missed - Wallaby logs the exact range and repairs it by re-backfill.',
    links: [
      { text: 'slot-loss gap detection', href: '/why-wallaby#slot-loss-gap-detection' },
      { text: 'configuration', href: '/configuration' },
    ],
  },
  {
    id: 'meili', x: 26, y: 674, w: 196, h: 59,
    title: 'meilisearch',
    subs: [],
    detail: 'Sinks receive batched documents and upsert by document id. That idempotency is the other half of the delivery guarantee: at-least-once redelivery converges to exactly-once results.',
    links: [{ text: 'meilisearch sink', href: '/sinks/meilisearch' }],
  },
  {
    id: 'http', x: 254, y: 674, w: 196, h: 59,
    title: 'http (webhook)',
    subs: [],
    detail: 'The HTTP sink posts signed batches to your endpoint - same contract: upsert by id, tolerate redelivery. Any system you can reach over HTTP can be kept in sync.',
    links: [{ text: 'http sink', href: '/sinks/http' }],
  },
  {
    id: 'kafka', x: 482, y: 674, w: 196, h: 59,
    title: 'kafka',
    subs: [],
    detail: 'The Kafka sink produces each document to its destination topic, keyed by document id so compaction and consumer-side upserts line up with the delivery contract. Deletes become tombstones.',
    links: [{ text: 'kafka sink', href: '/sinks/kafka' }],
  },
  {
    id: 'ack', x: 232, y: 769, w: 240, h: 77,
    title: 'acknowledge + checkpoint',
    subs: ['after every sink accepts', 'advance slot ▸ save lsn'],
    detail: 'The commit is acknowledged only after every sink accepted its batches - that single ordering rule is the at-least-once guarantee. A crash anywhere earlier simply re-streams from the last acknowledged position.',
    links: [],
  },
];

export const groups: IntGroup[] = [
  { id: 'g-pg', label: 'postgres', x: 16, y: 8, w: 216, h: 396 },
  { id: 'g-wb', label: 'wallaby leader', x: 256, y: 8, w: 216, h: 562 },
  { id: 'g-jobs', label: 'leader jobs', x: 490, y: 108, w: 200, h: 296 },
  { id: 'g-state', label: 'wallaby schema', x: 490, y: 428, w: 200, h: 200 },
  { id: 'g-sinks', label: 'sinks', x: 16, y: 650, w: 666, h: 100 },
];

export const edges: IntEdge[] = [
  { id: 'tables-wal', points: [[124, 103], [124, 127]] },
  { id: 'wal-pub', points: [[124, 186], [124, 210]] },
  { id: 'pub-slot', points: [[124, 287], [124, 311]] },
  {
    id: 'slot-stream', points: [[222, 349], [240, 349], [240, 73], [266, 73]],
    label: 'logical replication', lx: 245, ly: 300, vertical: true,
  },
  { id: 'stream-assemble', points: [[364, 103], [364, 127]] },
  { id: 'assemble-materialize', points: [[364, 204], [364, 228]] },
  { id: 'materialize-route', points: [[364, 287], [364, 311]] },
  { id: 'route-transform', points: [[364, 370], [364, 394]] },
  { id: 'transform-dispatch', points: [[364, 471], [364, 495]] },
  { id: 'route-fanout', points: [[462, 340], [496, 340]] },
  { id: 'fanout-transform', points: [[496, 368], [480, 368], [480, 410], [462, 410]] },
  {
    id: 'fanout-queue', points: [[590, 388], [590, 452]],
    label: 'offload tail', lx: 600, ly: 392,
  },
  {
    id: 'queue-backfill', points: [[684, 490], [696, 490], [696, 166], [684, 166]],
    label: 'notify', lx: 685, ly: 400, vertical: true,
  },
  {
    id: 'backfill-materialize', points: [[496, 166], [480, 166], [480, 257], [462, 257]],
    label: 'snapshot rows', lx: 484, ly: 180, vertical: true,
  },
  {
    id: 'backfill-tables', points: [[566, 127], [566, 26], [168, 26], [168, 44]],
    dashed: true, label: 'keyset reads', lx: 320, ly: 30,
  },
  { id: 'dispatch-meili', points: [[300, 554], [300, 644], [124, 644], [124, 674]] },
  { id: 'dispatch-http', points: [[352, 554], [352, 674]] },
  { id: 'dispatch-kafka', points: [[404, 554], [404, 644], [580, 644], [580, 674]] },
  { id: 'meili-ack', points: [[124, 733], [124, 758], [300, 758], [300, 769]] },
  { id: 'http-ack', points: [[352, 733], [352, 769]] },
  { id: 'kafka-ack', points: [[580, 733], [580, 758], [404, 758], [404, 769]] },
  {
    id: 'ack-slot', points: [[232, 807], [8, 807], [8, 349], [26, 349]],
    label: 'advance slot', lx: 40, ly: 812,
  },
  {
    id: 'ack-checkpoint', points: [[472, 807], [696, 807], [696, 573], [684, 573]],
    label: 'save checkpoint', lx: 500, ly: 812,
  },
];

const ALL_SINK_DROPS = ['dispatch-meili', 'dispatch-http', 'dispatch-kafka'];
const ALL_SINK_ACKS = ['meili-ack', 'http-ack', 'kafka-ack'];
const SINKS = ['meili', 'http', 'kafka'];

export const scenarios: IntScenario[] = [
  {
    id: 'live',
    label: 'live change',
    blurb: 'follow one committed transaction from the WAL to acknowledged delivery',
    steps: [
      { nodes: ['tables'], caption: 'an application transaction commits - any writer counts: your app, a migration, a bulk update' },
      { nodes: ['wal'], edges: ['tables-wal'], fx: 'tick', caption: 'every row change is already in the write-ahead log, in commit order' },
      { nodes: ['pub'], edges: ['wal-pub'], caption: 'the publication filters to captured tables - and only the columns your mappings consume' },
      { nodes: ['slot'], edges: ['pub-slot'], caption: 'the replication slot decodes via pgoutput and retains WAL until wallaby confirms delivery' },
      { nodes: ['stream'], edges: ['slot-stream'], caption: 'the elected leader streams from the slot, answering keepalives with its flushed position' },
      { nodes: ['assemble'], edges: ['stream-assemble'], caption: 'messages assemble into whole committed transactions; oversized ones spill out of memory' },
      { nodes: ['materialize'], edges: ['assemble-materialize'], caption: 'each row change materializes into your entity type through the storage provider' },
      { nodes: ['route'], edges: ['materialize-route'], caption: 'changes group by entity mapping and route to every sink that declared them' },
      { nodes: ['transform'], edges: ['route-transform'], caption: 'transforms shape the output documents, then batches are sliced to maxbatchsize' },
      { nodes: ['dispatch'], edges: ['transform-dispatch'], caption: 'the dispatcher writes independent sinks concurrently - retry with backoff, never skip' },
      { nodes: SINKS, edges: ALL_SINK_DROPS, fx: 'deliver', caption: 'each sink upserts by document id - idempotent, so a redelivery is harmless' },
      { nodes: ['ack'], edges: ALL_SINK_ACKS, caption: 'only after every sink accepts does wallaby acknowledge the transaction' },
      { nodes: ['slot', 'checkpoint'], edges: ['ack-slot', 'ack-checkpoint'], fx: 'flush', caption: 'the slot\'s flushed lsn advances and wallaby.checkpoint records it - at-least-once, end to end' },
    ],
  },
  {
    id: 'backfill',
    label: 'backfill',
    blurb: 'seed a new sink (or reindex) through the same pipeline as live changes',
    steps: [
      { nodes: ['backfill'], blue: true, caption: 'a mapping\'s backfill version changes, or a manual request arrives - a snapshot is scheduled' },
      { nodes: ['backfill', 'tables'], edges: ['backfill-tables'], blue: true, caption: 'the keyset pager reads chunks in primary-key order - resumable, with progress persisted per table' },
      { nodes: ['materialize'], edges: ['backfill-materialize'], blue: true, caption: 'snapshot rows enter the pipeline exactly where live rows do' },
      { nodes: ['route'], edges: ['materialize-route'], blue: true, caption: 'each chunk is bracketed by watermarks - a row changed live inside the window wins over its stale snapshot copy' },
      { nodes: ['transform'], edges: ['route-transform'], blue: true, caption: 'the same transforms run - there is no separate reindex code path to maintain' },
      { nodes: ['dispatch'], edges: ['transform-dispatch'], blue: true, caption: '…and the same dispatcher delivers the batches' },
      { nodes: SINKS, edges: ALL_SINK_DROPS, fx: 'deliver', blue: true, caption: 'documents arrive flagged as reads and upsert by id' },
      { nodes: ['slot'], blue: true, caption: 'no acknowledgement rides back - a snapshot never moves the slot' },
    ],
  },
  {
    id: 'fanout',
    label: 'fan-out',
    blurb: 'a change to a related table re-emits every entity that depends on it',
    steps: [
      { nodes: ['tables'], caption: 'a row in a dependent table changes - say, a category rename that touches a million products' },
      { nodes: ['wal', 'pub', 'slot'], edges: ['tables-wal', 'wal-pub', 'pub-slot'], fx: 'tick', caption: 'the dependent table is in the publication too, narrowed to just its key columns' },
      { nodes: ['stream', 'assemble', 'materialize'], edges: ['slot-stream', 'stream-assemble', 'assemble-materialize'], caption: 'it decodes like any other change…' },
      { nodes: ['route', 'fanout'], edges: ['materialize-route', 'route-fanout'], caption: '…but the router recognizes a dependency and hands it to fan-out' },
      { nodes: ['fanout', 'tables'], caption: 'one consolidated in (…) lookup resolves every affected primary key for the whole transaction' },
      { nodes: ['transform'], edges: ['fanout-transform'], caption: 'the first maxbatchsize entities re-emit inline as synthetic updates' },
      { nodes: ['dispatch', ...SINKS], edges: ['transform-dispatch', ...ALL_SINK_DROPS], fx: 'deliver', caption: '…and deliver through the normal path' },
      { nodes: ['ack', 'slot'], edges: [...ALL_SINK_ACKS, 'ack-slot'], fx: 'flush', caption: 'the trigger transaction acknowledges immediately - even with a huge tail still outstanding' },
      { nodes: ['queue'], edges: ['fanout-queue'], blue: true, caption: 'that tail was offloaded to the fan-out queue, coalescing repeat changes to the same rows' },
      { nodes: ['backfill'], edges: ['queue-backfill'], blue: true, caption: 'notify wakes the worker the instant a job lands; it re-snapshots the tail as a scoped backfill' },
      { nodes: ['materialize'], edges: ['backfill-materialize'], fx: 'deliver', blue: true, caption: 'the tail converges through the same pipeline - eventually consistent, absorbed by idempotent upserts' },
    ],
  },
  {
    id: 'slotloss',
    label: 'slot loss',
    blurb: 'the replication slot vanishes - detect the gap, repair it, converge',
    steps: [
      { nodes: ['slot'], warn: ['slot'], caption: 'the slot is gone - invalidated under wal pressure, lost in a failover, or dropped by hand' },
      { nodes: ['stream'], warn: ['slot'], caption: 'a fresh slot only streams forward from its creation point - on its own, the gap would be silent' },
      { nodes: ['checkpoint', 'stream'], warn: ['slot'], caption: 'wallaby.checkpoint is behind the new slot\'s consistent point: changes were missed, and the exact lsn range is logged' },
      { nodes: ['backfill'], blue: true, caption: 'every mapped table is marked for re-backfill automatically' },
      { nodes: ['backfill', 'tables'], edges: ['backfill-tables'], blue: true, caption: 'keyset snapshots re-read the tables…' },
      { nodes: ['materialize', 'route', 'transform', 'dispatch'], edges: ['backfill-materialize', 'materialize-route', 'route-transform', 'transform-dispatch'], blue: true, caption: '…and replay through the standard pipeline' },
      { nodes: SINKS, edges: ALL_SINK_DROPS, fx: 'deliver', blue: true, caption: 'idempotent upserts converge every sink - the gap is closed' },
    ],
  },
];
