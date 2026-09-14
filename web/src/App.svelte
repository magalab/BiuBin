<script lang="ts">
  import mqtt, { type MqttClient } from 'mqtt';
  import { onDestroy, onMount } from 'svelte';

  type Ports = {
    http?: number;
    grpc_h2c?: number;
    grpc_tls?: number;
    tcp?: number;
    udp?: number;
    mqtt_tcp?: number;
    mqtt_auth_tcp?: number;
    mqtt_v5?: number;
    mqtt_tls?: number;
    mqtt_ws?: number;
    thrift?: number;
  };

  type Info = {
    name: string;
    version: string;
    bind_host: string;
    advertise_host: string;
    ports: Ports;
    advertised_addresses?: Record<string, string | null>;
    mqtt_enabled: boolean;
    mqtt_tls_enabled: boolean;
    ready: boolean;
  };

  type BiubinEvent = {
    id: string;
    seq: number;
    at: string;
    protocol: string;
    kind: string;
    summary: string;
  };

  let info: Info | null = null;
  let events: BiubinEvent[] = [];
  let loadError = '';
  let copyNotice = '';
  let busy = false;
  let httpPath = '/anything/from-the-browser?source=biubin';
  let httpResult = '';
  let graphqlQuery = '{ echo(input: { message: "hello from GraphQL", repeat: 2 }) { message repeated } }';
  let graphqlVariables = '{}';
  let graphqlResult = '';
  let ws: WebSocket | null = null;
  let wsState = 'disconnected';
  let wsMessage = 'hello from the browser';
  let wsMessages: string[] = [];
  let sse: EventSource | null = null;
  let sseState = 'stopped';
  let sseMessages: string[] = [];
  let mqttClient: MqttClient | null = null;
  let mqttState = 'disconnected';
  let mqttTopic = 'biubin/browser/demo';
  let mqttPayload = 'hello over MQTT';
  let mqttQos: 0 | 1 | 2 = 0;
  let mqttRetain = false;
  let mqttMessages: string[] = [];

  $: httpUrl = `${window.location.origin}${httpPath}`;
  $: wsUrl = `${window.location.origin.replace(/^http/, 'ws')}/ws/echo`;
  $: mqttUrl = info
    ? `ws://${info.advertised_addresses?.mqtt_ws ?? `${info.advertise_host}:${info.ports.mqtt_ws ?? 8083}`}`
    : 'ws://127.0.0.1:8083';

  function address(protocol: string, fallbackHost: string, fallbackPort: number) {
    return info?.advertised_addresses?.[protocol] ?? `${info?.advertise_host ?? fallbackHost}:${info?.ports[protocol as keyof Ports] ?? fallbackPort}`;
  }

  async function loadControlPlane() {
    try {
      const [infoResponse, eventsResponse] = await Promise.all([
        fetch('/api/v1/info'),
        fetch('/api/v1/events?limit=20')
      ]);
      if (!infoResponse.ok || !eventsResponse.ok) throw new Error('control plane request failed');
      info = (await infoResponse.json()) as Info;
      events = ((await eventsResponse.json()) as { events: BiubinEvent[] }).events;
      loadError = '';
    } catch (error) {
      loadError = error instanceof Error ? error.message : 'unable to reach biubin';
    }
  }

  async function copy(value: string) {
    try {
      await navigator.clipboard.writeText(value);
      copyNotice = 'copied';
      window.setTimeout(() => (copyNotice = ''), 1600);
    } catch {
      copyNotice = 'copy unavailable';
    }
  }

  async function callHttp() {
    busy = true;
    httpResult = '';
    try {
      const response = await fetch(httpUrl);
      httpResult = `${response.status} ${response.statusText}\n${JSON.stringify(await response.json(), null, 2)}`;
    } catch (error) {
      httpResult = error instanceof Error ? error.message : 'request failed';
    } finally {
      busy = false;
      await loadControlPlane();
    }
  }

  async function callGraphql() {
    graphqlResult = '';
    try {
      const variables = JSON.parse(graphqlVariables) as Record<string, unknown>;
      const response = await fetch('/graphql', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ query: graphqlQuery, variables })
      });
      graphqlResult = `${response.status} ${response.statusText}\n${JSON.stringify(await response.json(), null, 2)}`;
    } catch (error) {
      graphqlResult = error instanceof Error ? error.message : 'GraphQL request failed';
    } finally {
      await loadControlPlane();
    }
  }

  function connectWs() {
    if (ws) return;
    ws = new WebSocket(wsUrl);
    wsState = 'connecting';
    ws.onopen = () => (wsState = 'connected');
    ws.onmessage = (event) => {
      wsMessages = [`${new Date().toLocaleTimeString()}  ${String(event.data)}`, ...wsMessages].slice(0, 20);
    };
    ws.onerror = () => (wsState = 'error');
    ws.onclose = () => {
      wsState = 'disconnected';
      ws = null;
    };
  }

  function sendWs() {
    if (ws?.readyState === WebSocket.OPEN) ws.send(wsMessage);
  }

  function disconnectWs() {
    ws?.close(1000, 'browser closed');
    ws = null;
    wsState = 'disconnected';
  }

  function startSse() {
    if (sse) return;
    sseMessages = [];
    sse = new EventSource('/sse/ticker?interval_ms=500&count=8');
    sseState = 'connecting';
    sse.onopen = () => (sseState = 'connected');
    const recordSseMessage = (event: MessageEvent<string>) => {
      sseMessages = [`${event.lastEventId || '-'}  ${event.data}`, ...sseMessages].slice(0, 20);
    };
    sse.onmessage = recordSseMessage;
    sse.addEventListener('tick', recordSseMessage as EventListener);
    sse.onerror = () => {
      sseState = 'stopped';
      sse?.close();
      sse = null;
    };
  }

  function stopSse() {
    sse?.close();
    sse = null;
    sseState = 'stopped';
  }

  function connectMqtt() {
    if (mqttClient || !info?.mqtt_enabled) return;
    mqttMessages = [];
    mqttState = 'connecting';
    mqttClient = mqtt.connect(mqttUrl, {
      clientId: `biubin-web-${Math.random().toString(16).slice(2)}`,
      reconnectPeriod: 0,
      clean: true
    });
    mqttClient.on('connect', () => {
      mqttState = 'connected';
      mqttClient?.subscribe(mqttTopic);
    });
    mqttClient.on('message', (topic, payload) => {
      mqttMessages = [`${topic}  ${payload.toString()}`, ...mqttMessages].slice(0, 20);
    });
    mqttClient.on('error', (error) => {
      mqttState = `error: ${error.message}`;
      mqttClient?.end(true);
      mqttClient = null;
    });
    mqttClient.on('close', () => {
      mqttState = 'disconnected';
      mqttClient = null;
    });
  }

  function publishMqtt() {
    if (!mqttClient || mqttState !== 'connected') return;
    mqttClient.publish(mqttTopic, mqttPayload, { qos: mqttQos, retain: mqttRetain });
  }

  function disconnectMqtt() {
    mqttClient?.end(true);
    mqttClient = null;
    mqttState = 'disconnected';
  }

  onMount(() => {
    void loadControlPlane();
    const timer = window.setInterval(() => void loadControlPlane(), 5000);
    return () => {
      window.clearInterval(timer);
      disconnectWs();
      stopSse();
      disconnectMqtt();
    };
  });

  onDestroy(() => {
    disconnectWs();
    stopSse();
    disconnectMqtt();
  });
</script>

<svelte:head>
  <title>biubin · protocol fixture</title>
</svelte:head>

<main>
  <header class="hero">
    <div>
      <p class="eyebrow">LOCAL-FIRST PROTOCOL FIXTURE</p>
      <h1>biubin<span>•</span></h1>
      <p class="lede">A small, deterministic playground for clients, SDKs, gateways and proxies.</p>
    </div>
    <div class="status-card" class:ready={info?.ready}>
      <span class="status-dot"></span>
      <div>
        <strong>{info?.ready ? 'ready' : 'starting'}</strong>
        <small>{info ? `v${info.version} · ${info.advertise_host}` : 'loading control plane'}</small>
      </div>
    </div>
  </header>

  {#if loadError}
    <div class="notice error">{loadError}. Is the <code>biubin</code> process running?</div>
  {/if}

  <section class="grid overview">
    <article class="panel wide">
      <div class="panel-heading">
        <div><p class="eyebrow">CONNECTIONS</p><h2>Every protocol, one process</h2></div>
        <span class="pill">{info?.name ?? 'biubin'}</span>
      </div>
      <div class="connection-list">
        <div><span>HTTP / WS / SSE / GraphQL</span><code>{info ? address('http', '127.0.0.1', 8080) : '—'}</code></div>
        <div><span>gRPC h2c / TLS</span><code>{info ? address('grpc_h2c', '127.0.0.1', 9000) : '—'}</code></div>
        <div><span>TCP / UDP</span><code>{info ? address('tcp', '127.0.0.1', 7000) : '—'} / {info ? address('udp', '127.0.0.1', 7001) : '—'}</code></div>
        <div><span>Thrift binary</span><code>{info ? address('thrift', '127.0.0.1', 9090) : '—'}</code></div>
        <div><span>MQTT WebSocket</span><code>{info?.mqtt_enabled ? mqttUrl : 'disabled by default'}</code></div>
      </div>
    </article>
    <article class="panel">
      <p class="eyebrow">QUICK LINKS</p>
      <div class="link-stack">
        <a href="/api/v1/info" target="_blank">info API ↗</a>
        <a href="/api/v1/capabilities" target="_blank">capabilities ↗</a>
        <a href="/openapi" target="_blank">OpenAPI docs ↗</a>
        <a href="/graphql" target="_blank">GraphQL endpoint ↗</a>
        <button class="text-button" onclick={() => copy(`curl ${window.location.origin}/anything/demo`)}>copy curl</button>
      </div>
      {#if copyNotice}<small class="copy-notice">{copyNotice}</small>{/if}
    </article>
  </section>

  <section class="grid demos">
    <article class="panel">
      <div class="panel-heading"><div><p class="eyebrow">HTTP</p><h2>Echo a request</h2></div><span class="method">GET</span></div>
      <label>Path <input bind:value={httpPath} /></label>
      <div class="button-row"><button onclick={callHttp} disabled={busy}>{busy ? 'calling…' : 'call endpoint'}</button><button class="secondary" onclick={() => copy(`curl ${httpUrl}`)}>copy curl</button></div>
      {#if httpResult}<pre>{httpResult}</pre>{/if}
    </article>

    <article class="panel">
      <div class="panel-heading"><div><p class="eyebrow">WEBSOCKET</p><h2>Frame echo</h2></div><span class="pill">{wsState}</span></div>
      <label>Message <input bind:value={wsMessage} /></label>
      <div class="button-row"><button onclick={connectWs} disabled={!!ws}>connect</button><button onclick={sendWs} disabled={wsState !== 'connected'}>send</button><button class="secondary" onclick={disconnectWs}>close</button></div>
      {#if wsMessages.length}<pre>{wsMessages.join('\n')}</pre>{/if}
    </article>

    <article class="panel">
      <div class="panel-heading"><div><p class="eyebrow">GRAPHQL</p><h2>Run a query</h2></div><span class="method">POST</span></div>
      <label>Query <textarea bind:value={graphqlQuery} rows="4"></textarea></label>
      <label>Variables JSON <input bind:value={graphqlVariables} /></label>
      <div class="button-row"><button onclick={callGraphql}>run query</button><button class="secondary" onclick={() => copy(`${window.location.origin}/graphql`)}>copy endpoint</button></div>
      {#if graphqlResult}<pre>{graphqlResult}</pre>{/if}
    </article>

    <article class="panel">
      <div class="panel-heading"><div><p class="eyebrow">SERVER-SENT EVENTS</p><h2>Watch a ticker</h2></div><span class="pill">{sseState}</span></div>
      <p class="muted">A finite stream makes reconnect and cancellation behavior easy to inspect.</p>
      <div class="button-row"><button onclick={startSse} disabled={!!sse}>start</button><button class="secondary" onclick={stopSse}>stop</button></div>
      {#if sseMessages.length}<pre>{sseMessages.join('\n')}</pre>{/if}
    </article>

    <article class="panel">
      <div class="panel-heading"><div><p class="eyebrow">MQTT OVER WEBSOCKET</p><h2>Publish / subscribe</h2></div><span class="pill">{info?.mqtt_enabled ? mqttState : 'disabled'}</span></div>
      <label>Topic <input bind:value={mqttTopic} /></label>
      <label>Payload <input bind:value={mqttPayload} /></label>
      <div class="inline-fields"><label>QoS <select bind:value={mqttQos}><option value={0}>0</option><option value={1}>1</option><option value={2}>2</option></select></label><label class="check"><input type="checkbox" bind:checked={mqttRetain} /> retain</label></div>
      <div class="button-row"><button onclick={connectMqtt} disabled={!info?.mqtt_enabled || !!mqttClient}>connect</button><button onclick={publishMqtt} disabled={mqttState !== 'connected'}>publish</button><button class="secondary" onclick={disconnectMqtt}>close</button></div>
      <p class="muted">Enable MQTT with <code>BIUBIN_MQTT_ENABLED=true</code>; the browser panel uses the anonymous WS listener.</p>
      {#if mqttMessages.length}<pre>{mqttMessages.join('\n')}</pre>{/if}
    </article>
  </section>

  <section class="grid docs-events">
    <article class="panel">
      <div class="panel-heading"><div><p class="eyebrow">PROTOCOL NOTES</p><h2>Copyable starting points</h2></div></div>
      <div class="doc-row"><strong>GraphQL</strong><code>POST /graphql · WS /graphql/ws</code><button class="text-button" onclick={() => copy(`curl -s ${window.location.origin}/graphql -H 'content-type: application/json' --data '{"query":"{ serverInfo { name version } }'}`)}>copy</button></div>
      <div class="doc-row"><strong>Thrift</strong><code>thrift://{info ? address('thrift', '127.0.0.1', 9090) : '127.0.0.1:9090'}</code><button class="text-button" onclick={() => copy('thrift/biubin.thrift')}>IDL</button></div>
      <div class="doc-row"><strong>Socket</strong><code>TCP line or 4-byte big-endian length prefix</code><button class="text-button" onclick={() => copy(`nc ${info ? address('tcp', '127.0.0.1', 7000) : '127.0.0.1:7000'}`)}>copy</button></div>
      <div class="doc-row"><strong>gRPC</strong><code>reflection enabled on h2c and TLS listeners</code><button class="text-button" onclick={() => copy(`grpcurl ${info ? address('grpc_h2c', '127.0.0.1', 9000) : '127.0.0.1:9000'} list`)}>copy</button></div>
    </article>
    <article class="panel events-panel">
      <div class="panel-heading"><div><p class="eyebrow">RECENT EVENTS</p><h2>Live fixture activity</h2></div><span class="pill">{events.length}/200</span></div>
      {#if events.length}
        <div class="events-list">
          {#each events as event (event.id)}
            <div class="event"><span class="event-time">{new Date(event.at).toLocaleTimeString()}</span><span class="event-protocol">{event.protocol}</span><span>{event.summary || event.kind}</span></div>
          {/each}
        </div>
      {:else}
        <p class="muted">No events yet. Try an endpoint above.</p>
      {/if}
      <a class="small-link" href="/api/v1/events" target="_blank">open raw event feed ↗</a>
    </article>
  </section>

  <footer>biubin is a development and integration-test fixture. Do not expose default credentials or certificates to production.</footer>
</main>
