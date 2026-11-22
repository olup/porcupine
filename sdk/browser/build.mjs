import { copyFile, readFile, writeFile } from 'node:fs/promises'
import { build } from 'esbuild'

const version = (await readFile(new URL('../../VERSION', import.meta.url), 'utf8')).trim()
const manifest = {
  name: '@porcupine/browser',
  version,
  description: 'Cookieless browser analytics for Porcupine',
  type: 'module',
  license: 'MIT',
  repository: 'github:olup/porcupine',
  main: './index.js',
  types: './index.d.ts',
  exports: {
    '.': { types: './index.d.ts', import: './index.js' },
    './porcupine.js': './porcupine.js',
  },
  files: ['index.js', 'index.d.ts', 'porcupine.js', 'LICENSE', 'README.md'],
}
const options = {
  entryPoints: ['src/index.ts'],
  bundle: true,
  target: 'es2020',
}

await Promise.all([
  build({ ...options, format: 'esm', outfile: 'dist/index.js' }),
  build({
    ...options,
    format: 'iife',
    globalName: 'Porcupine',
    minify: true,
    outfile: 'dist/porcupine.js',
  }),
  copyFile(new URL('../../LICENSE', import.meta.url), 'dist/LICENSE'),
  copyFile('README.md', 'dist/README.md'),
  writeFile('dist/package.json', JSON.stringify(manifest, null, 2) + '\n'),
])
