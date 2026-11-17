// Test controls and host service credentials belong to the driver, never the fixture server.
export function serverEnv(overrides, inherited = process.env) {
  return {
    ...Object.fromEntries(Object.entries(inherited).filter(([name]) => !/^(PORCUPINE_|AWS_)/i.test(name))),
    ...overrides,
  };
}
