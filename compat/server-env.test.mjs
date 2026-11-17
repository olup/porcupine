import { test } from 'node:test';
import assert from 'node:assert/strict';
import { serverEnv } from './server-env.mjs';

test('SDK fixture strips host service credentials and driver-only canary flags', () => {
  const parent = { PATH: '/go/bin', HOME: '/home/test', PORCUPINE_UNEXPECTED: 'sentinel',
    PORCUPINE_COMPAT_CANARY: '1', PORCUPINE_UPDATE_FIXTURES: '1', PORCUPINE_DB: '/private/real.db',
    AWS_ACCESS_KEY_ID: 'private-key', AWS_PROFILE: 'production', aws_secret_access_key: 'private-secret' };
  assert.deepEqual(serverEnv({ PORCUPINE_DB: '/tmp/test.db', PORCUPINE_BOOTSTRAP_TOKEN: 'synthetic' }, parent),
    { PATH: '/go/bin', HOME: '/home/test', PORCUPINE_DB: '/tmp/test.db', PORCUPINE_BOOTSTRAP_TOKEN: 'synthetic' });
  assert.equal(parent.PORCUPINE_DB, '/private/real.db');
});
