const {test, expect} = require('@playwright/test');
const fs = require('node:fs');
const fixture = JSON.parse(fs.readFileSync(process.env.BROWSER_FIXTURE, 'utf8'));

test.beforeEach(async ({page}) => {
  await page.goto(fixture.page);
  await expect.poll(() => page.evaluate(() => !!globalThis.grpcWebFixture)).toBe(true);
});

for (const protocol of ['h1', 'h2', 'async']) {
  for (const mode of ['binary', 'text']) {
    test(`${protocol} official generated ${mode} unary typed payload, metadata, status and end`, async ({page, request}) => {
      const result = await page.evaluate(async ({origin, mode}) => {
        const {messages, [mode]: generated} = grpcWebFixture;
        const client = new generated.PeerClient(origin);
        const message = new messages.Message();
        message.setValue('browser unary');
        message.setPayload(new Uint8Array([0, 255, 127]));
        return new Promise(resolve => {
          const events = [];
          const call = client.unary(message, {}, (error, response) => {
            events.push(['callback', error?.code ?? 0, response?.getValue(), Array.from(response?.getPayload_asU8() ?? [])]);
          });
          call.on('status', status => events.push(['status', status.code, status.metadata]));
          call.on('metadata', metadata => events.push(['metadata', metadata]));
          call.on('end', () => resolve(events));
        });
      }, {origin: fixture[protocol], mode});
      expect(result.find(event => event[0] === 'callback').slice(1)).toEqual([0, 'browser unary', [0, 255, 127]]);
      expect(result.find(event => event[0] === 'status')[1]).toBe(0);
      expect(result.find(event => event[0] === 'metadata')[1]['x-leading']).toBe('visible');
      const state = await (await request.get(fixture.h1 + '/__state')).json();
      expect(state.requests.some(item => item.method === 'POST' && item.protocol === (protocol === 'h1' ? 'http1' : 'h2') && item.host === (protocol === 'async' ? 'async' : 'falcon'))).toBe(true);
    });
  }

  test(`${protocol} official text server streaming delivers first message before controlled handler release`, async ({page, request}) => {
    const id = `${protocol}-incremental`;
    await page.evaluate(({origin, id}) => {
      const {messages, text} = grpcWebFixture;
      const message = new messages.Message();
      message.setValue(id);
      const client = new text.PeerClient(origin);
      globalThis.streamEvents = [];
      const call = client.serverStream(message, {});
      call.on('data', value => streamEvents.push(['data', value.getValue()]));
      call.on('status', status => streamEvents.push(['status', status.code]));
      call.on('error', error => streamEvents.push(['error', error.code]));
      call.on('end', () => streamEvents.push(['end']));
    }, {origin: fixture[protocol], id});
    await expect.poll(() => page.evaluate(() => streamEvents)).toEqual([['data', 'first']]);
    const state = await (await request.get(fixture.h1 + '/__state')).json();
    expect(state.handlers).toContain(id);
    expect(state.completed).not.toContain(id);
    expect(state.releases).not.toContain(id);
    await request.post(fixture.h1 + '/__release?id=' + encodeURIComponent(id));
    await expect.poll(() => page.evaluate(() => streamEvents)).toEqual([
      ['data', 'first'], ['data', 'second'], ['status', 0], ['end'],
    ]);
  });
}

test('native Rails browser credentials, authorization and valid/invalid CSRF retain framework policy', async ({page, request}) => {
  await page.goto(fixture.rails + '/token');
  const token = await page.evaluate(() => JSON.parse(document.body.innerText).token);
  await page.addScriptTag({path: __dirname + '/../vendor/browser/client.js'});
  const invoke = async (metadata, credentials) => page.evaluate(async ({origin, metadata, credentials}) => {
    const {messages, text} = grpcWebFixture;
    const client = new text.PeerClient(origin, null, {'withCredentials': credentials});
    const message = new messages.Message();
    message.setValue('Rails browser');
    return new Promise(resolve => client.unary(message, metadata, (error, value) => resolve({
      code: error?.code ?? 0, value: value?.getValue(), message: error?.message,
    })));
  }, {origin: fixture.rails, metadata, credentials});
  const invalid = await invoke({'authorization': 'Bearer browser'}, true);
  expect(invalid.code).toBe(13);
  expect((await invoke({'authorization': 'wrong', 'x-csrf-token': token}, true)).code).toBe(16);
  expect(await invoke({'authorization': 'Bearer browser', 'x-csrf-token': token}, true)).toEqual({code: 0, value: 'Rails browser'});
  const before = await (await request.get(fixture.h1 + '/__state')).json();
  await page.goto(fixture.page);
  // CORS allows this origin and credentials, but Rails' real origin check still
  // rejects the cross-origin POST even with a valid session token.
  expect((await invoke({'authorization': 'Bearer browser', 'x-csrf-token': token}, true)).code).toBe(13);
  const after = await (await request.get(fixture.h1 + '/__state')).json();
  expect(after.rails.filter(event => event === 'handler').length).toBe(before.rails.filter(event => event === 'handler').length);
});

test('official generated client receives canonical rich Status and collapsed duplicate body trailers', async ({page}) => {
  const result = await page.evaluate(async origin => {
    const {messages, text, status} = grpcWebFixture;
    const message = new messages.Message();
    message.setValue('error');
    const client = new text.PeerClient(origin);
    return new Promise(resolve => {
      const events = [];
      const call = client.unary(message, {}, error => events.push(['error', error.code, error.message]));
      call.on('status', terminal => {
        const bytes = Uint8Array.from(atob(terminal.metadata['grpc-status-details-bin']), char => char.charCodeAt(0));
        const rich = status.Status.deserializeBinary(bytes);
        events.push(['status', terminal.code, rich.getCode(), rich.getMessage(),
          messages.Message.deserializeBinary(rich.getDetailsList()[0].getValue_asU8()).getValue(), terminal.metadata['x-duplicate']]);
      });
      call.on('end', () => { events.push(['end']); resolve(events); });
    });
  }, fixture.h1);
  expect(result[0]).toEqual(['error', 5, 'missing % café']);
  expect(result[1].slice(0, 6)).toEqual(['status', 5, 5, 'missing % café', 'error', 'two']);
});

test('denied true cross-origin preflight suppresses POST, handler and RPC lifecycle', async ({page, request}) => {
  await page.goto(fixture.denied_page);
  const before = await (await request.get(fixture.h1 + '/__state')).json();
  const code = await page.evaluate(async origin => {
    const {messages, text} = grpcWebFixture;
    const message = new messages.Message();
    message.setValue('must never execute');
    return new Promise(resolve => new text.PeerClient(origin).unary(message, {}, error => resolve(error.code)));
  }, fixture.h1);
  expect(code).not.toBe(0);
  const after = await (await request.get(fixture.h1 + '/__state')).json();
  expect(after.handlers).toEqual(before.handlers);
  expect(after.calls).toEqual(before.calls);
  expect(after.requests.filter(item => item.method === 'POST')).toEqual(before.requests.filter(item => item.method === 'POST'));
  expect(after.requests.filter(item => item.method === 'OPTIONS').length).toBeGreaterThan(before.requests.filter(item => item.method === 'OPTIONS').length);
});

test('credential denial distinguishes preflight suppression from unreadability after execution', async ({page, request}) => {
  const invoke = async (origin, id, credentials) => page.evaluate(async ({origin, id, credentials}) => {
    const {messages, text} = grpcWebFixture;
    const message = new messages.Message();
    message.setValue(id);
    return new Promise(resolve => new text.PeerClient(origin, null, {'withCredentials': credentials})
      .unary(message, {}, (error, value) => resolve({code: error?.code ?? 0, value: value?.getValue()})));
  }, {origin, id, credentials});
  expect(await invoke(fixture.no_credentials, 'uncredentialed positive', false)).toEqual({code: 0, value: 'uncredentialed positive'});
  expect((await invoke(fixture.no_credentials, 'credential preflight suppressed', true)).code).not.toBe(0);
  const suppressed = await (await request.get(fixture.h1 + '/__state')).json();
  expect(suppressed.handlers).not.toContain('credential preflight suppressed');
  expect((await invoke(fixture.unreadable, 'credential effect unreadable', true)).code).not.toBe(0);
  const unreadable = await (await request.get(fixture.h1 + '/__state')).json();
  expect(unreadable.handlers).toContain('credential effect unreadable');
});

test('cross-origin disallowed method and metadata headers are denied before RPC execution', async ({page, request}) => {
  const before = await (await request.get(fixture.h1 + '/__state')).json();
  const results = await page.evaluate(async origin => {
    const {messages, text} = grpcWebFixture;
    const message = new messages.Message();
    message.setValue('forbidden metadata');
    const headerCode = await new Promise(resolve => new text.PeerClient(origin)
      .unary(message, {'x-forbidden': 'no'}, error => resolve(error.code)));
    // This is a CORS verb rejection control, not an RPC serializer/transport.
    const methodDenied = await fetch(origin + '/connectrpc.grpcinterop.v1.Peer/Unary', {
      method: 'PUT', headers: {'content-type': 'application/grpc-web'}, body: '',
    }).then(() => false, () => true);
    return {headerCode, methodDenied};
  }, fixture.h1);
  expect(results.headerCode).not.toBe(0);
  expect(results.methodDenied).toBe(true);
  const after = await (await request.get(fixture.h1 + '/__state')).json();
  expect(after.handlers).toEqual(before.handlers);
  expect(after.calls).toEqual(before.calls);
  expect(after.requests.filter(item => item.method === 'POST')).toEqual(before.requests.filter(item => item.method === 'POST'));
});

test('independent grpc-go text reference executes official generated browser unary and stream', async ({page}) => {
  const result = await page.evaluate(async origin => {
    const {messages, text} = grpcWebFixture;
    const client = new text.PeerClient(origin);
    const message = new messages.Message();
    message.setValue('independent text');
    message.setPayload(new Uint8Array([0, 255]));
    const unary = await new Promise((resolve, reject) => client.unary(message, {}, (error, value) => {
      error ? reject(error) : resolve([value.getValue(), Array.from(value.getPayload_asU8())]);
    }));
    const stream = await new Promise(resolve => {
      const events = [];
      const call = client.serverStream(message, {});
      call.on('data', value => events.push(['data', value.getValue()]));
      call.on('status', status => events.push(['status', status.code]));
      call.on('error', error => events.push(['error', error.code]));
      call.on('end', () => { events.push(['end']); resolve(events); });
    });
    return {unary, stream};
  }, fixture.reference);
  expect(result).toEqual({unary: ['independent text', [0, 255]], stream: [
    ['data', 'independent text'], ['data', 'reference second'], ['status', 0], ['end'],
  ]});
});

test('binary XHR server-stream control buffers data until handler completion', async ({page, request}) => {
  const id = 'binary-xhr-buffered';
  const cdp = await page.context().newCDPSession(page);
  await cdp.send('Network.enable');
  const ids = new Set();
  let receivedBytes = 0;
  cdp.on('Network.requestWillBeSent', event => {
    if (event.request.url === fixture.h1 + '/connectrpc.grpcinterop.v1.Peer/ServerStream' && event.request.method === 'POST') ids.add(event.requestId);
  });
  cdp.on('Network.dataReceived', event => { if (ids.has(event.requestId)) receivedBytes += event.dataLength; });
  await page.evaluate(({origin, id}) => {
    const {messages, binary} = grpcWebFixture;
    const message = new messages.Message();
    message.setValue(id);
    globalThis.binaryEvents = [];
    const call = new binary.PeerClient(origin).serverStream(message, {});
    call.on('data', value => binaryEvents.push(['data', value.getValue()]));
    call.on('status', status => binaryEvents.push(['status', status.code]));
    call.on('error', error => binaryEvents.push(['error', error.code]));
    call.on('end', () => binaryEvents.push(['end']));
  }, {origin: fixture.h1, id});
  // The handler only waits after its first send transferred real bytes. This
  // independent control observes that boundary before checking absent callbacks.
  await expect.poll(async () => {
    const state = await (await request.get(fixture.h1 + '/__state')).json();
    return state.sent.includes(id);
  }).toBe(true);
  await expect.poll(() => receivedBytes).toBeGreaterThan(0);
  expect(await page.evaluate(() => binaryEvents)).toEqual([]);
  await request.post(fixture.h1 + '/__release?id=' + id);
  await expect.poll(() => page.evaluate(() => binaryEvents)).toEqual([
    ['data', 'first'], ['data', 'second'], ['status', 0], ['end'],
  ]);
  await cdp.detach();
});

for (const protocol of ['h1', 'h2', 'async']) {
  test(`${protocol} official text cancellation is silent after actual first data`, async ({page, request}) => {
    const id = `${protocol}-cancel`;
    await page.evaluate(({origin, id}) => {
      const {messages, text} = grpcWebFixture;
      const message = new messages.Message();
      message.setValue(id);
      globalThis.cancelEvents = [];
      globalThis.cancelCall = new text.PeerClient(origin).serverStream(message, {});
      cancelCall.on('data', value => cancelEvents.push(['data', value.getValue()]));
      cancelCall.on('status', status => cancelEvents.push(['status', status.code]));
      cancelCall.on('error', error => cancelEvents.push(['error', error.code]));
      cancelCall.on('end', () => cancelEvents.push(['end']));
    }, {origin: fixture[protocol], id});
    await expect.poll(() => page.evaluate(() => cancelEvents)).toEqual([['data', 'first']]);
    await page.evaluate(() => cancelCall.cancel());
    await request.post(fixture.h1 + '/__release?id=' + id);
    await expect.poll(async () => {
      const state = await (await request.get(fixture.h1 + '/__state')).json();
      return state.completed.includes(id);
    }).toBe(true);
    expect(await page.evaluate(() => cancelEvents)).toEqual([['data', 'first']]);
    const state = await (await request.get(fixture.h1 + '/__state')).json();
    if (protocol === 'h1') expect(state.cancellation.find(item => item.id === id).reason).toBe(null);
    // H2's observed reset, if any, is recorded by the real server Context. A JS
    // cancel action alone is never used to assert a peer CANCEL reset.
    console.log(protocol + ' observed server cancellation: ' + JSON.stringify(state.cancellation.find(item => item.id === id)));
  });
}
