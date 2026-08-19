// @ts-check
import { defineConfig } from 'astro/config';
import { unified } from '@astrojs/markdown-remark';
import starlight from '@astrojs/starlight';
import starlightLlmsTxt from 'starlight-llms-txt';
import { remarkPrependBase } from './src/plugins/remark-prepend-base.mjs';

const BASE = '/k8s-lookout';

// Stack mirrored from core-agent's docs/site (a maintainer of one
// repo should recognize the other): Astro Starlight, light-only
// theme, remark-prepend-base so content links stay decoupled from
// the deploy base.
//
// baseURL matches the production GH Pages path so relative links
// resolve identically in dev and in prod.
export default defineConfig({
  site: 'https://go-steer.github.io',
  base: BASE,
  markdown: {
    processor: unified({ remarkPlugins: [remarkPrependBase(BASE)] }),
  },
  // The sentinel coverage map moved out of Getting started when the
  // "What lookout detects" section landed (one coverage page per mode).
  // The old URL was linked from the README and shipped in a release, so
  // it redirects rather than 404s.
  redirects: {
    '/getting-started/what-the-sentinel-watches':
      '/k8s-lookout/detect/sentinel/',
  },
  integrations: [
    starlight({
      title: 'k8s-lookout',
      description:
        'Data-plane intelligence for core-agent: deterministic, token-dense eyes on Kubernetes/GKE clusters for LLM-driven troubleshooting agents.',
      logo: undefined,
      social: [
        {
          icon: 'github',
          label: 'GitHub',
          href: 'https://github.com/go-steer/k8s-lookout',
        },
      ],
      editLink: {
        baseUrl:
          'https://github.com/go-steer/k8s-lookout/edit/main/docs/site/',
      },
      // Inline script runs before Starlight's own ThemeProvider script,
      // pinning data-theme to 'light' before first paint. Belt-and-braces
      // with the theme.css overrides that already apply under both
      // [data-theme='light'] and [data-theme='dark']. (Mirrors core-agent.)
      head: [
        {
          tag: 'script',
          attrs: { 'is:inline': true },
          content: "document.documentElement.dataset.theme = 'light';",
        },
      ],
      // llms.txt / llms-full.txt / llms-small.txt at the site root,
      // built from the same content collection as the pages — the
      // agent-facing mirror of the site (llmstxt.org convention).
      // /agents/ is the curated entry point and sorts first.
      plugins: [
        starlightLlmsTxt({
          projectName: 'k8s-lookout',
          description:
            'Cluster diagnostics for AI troubleshooting agents: one binary with three surfaces — the lookout CLI, an MCP server (lookout mcp), and an in-cluster sentinel (lookout watch). Read-only against the cluster, secret-safe, token-dense output. Start at "Using k8s-lookout from an AI agent" for install and setup tasks.',
          promote: ['agents', 'getting-started/**'],
          demote: ['reference/**'],
        }),
      ],
      // Palette + typography live in one file so the whole visual
      // system is swappable.
      customCss: ['./src/styles/theme.css'],
      // Empty component overrides drop the dark-mode toggle from the
      // navbar. Light-only site, same as core-agent.
      components: {
        ThemeSelect: './src/components/ThemeSelect.astro',
        ThemeProvider: './src/components/ThemeProvider.astro',
      },
      // Sections use `autogenerate` so new pages appear automatically
      // once added under the source dir. Reference is generated-only
      // (dev/tools/gen-site-docs); the other sections are scaffolded
      // in this PR and filled in follow-ups.
      sidebar: [
        {
          label: 'Overview',
          items: [
            { label: 'Introduction', link: '/' },
            { label: 'For AI agents', link: '/agents/' },
          ],
        },
        {
          label: 'Getting started',
          items: [{ autogenerate: { directory: 'getting-started' } }],
        },
        {
          label: 'What lookout detects',
          items: [{ autogenerate: { directory: 'detect' } }],
        },
        {
          label: 'Concepts',
          items: [{ autogenerate: { directory: 'concepts' } }],
        },
        {
          label: 'Guides',
          items: [{ autogenerate: { directory: 'guides' } }],
        },
        {
          label: 'Reference',
          items: [{ autogenerate: { directory: 'reference' } }],
        },
        {
          label: 'Operations',
          items: [{ autogenerate: { directory: 'operations' } }],
        },
        {
          label: 'Contributing',
          items: [{ autogenerate: { directory: 'contributing' } }],
        },
      ],
    }),
  ],
});
