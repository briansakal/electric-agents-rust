import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: [`conformance/agents.integration.ts`],
    fileParallelism: false,
    testTimeout: 30_000,
    hookTimeout: 60_000,
  },
})
