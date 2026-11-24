// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Client-side Mermaid rendering for k8s-lookout docs (bundled via Vite so
// zero external CDN requests occur at build or page-load time, and dynamically
// imported only on pages containing ```mermaid blocks).
//
// Starlight's default handling renders ```mermaid blocks as syntax-highlighted
// code (via expressive-code). This script upgrades them to rendered SVG
// diagrams, adds an Expand button that opens the diagram in a full-viewport
// modal, and preserves the original source in a collapsible <details> block.

let mermaidPromise = null;

async function loadMermaid() {
  if (!mermaidPromise) {
    mermaidPromise = import('mermaid').then(({ default: mermaid }) => {
      mermaid.initialize({
        startOnLoad: false,
        theme: 'base',
        // Prevent Mermaid from appending orphaned error bomb SVGs to
        // document.body before throwing on a parse/render failure.
        suppressErrorRendering: true,
        securityLevel: 'strict',
        flowchart: { htmlLabels: true, curve: 'basis' },
        themeVariables: {
          background: '#f6f8fa',
          primaryColor: '#e6f7fb',
          primaryTextColor: '#0f1721',
          primaryBorderColor: '#0092b8',
          secondaryColor: '#eef8f6',
          secondaryBorderColor: '#1fa093',
          tertiaryColor: '#ffffff',
          tertiaryBorderColor: '#c8ced8',
          lineColor: '#556170',
          textColor: '#1e2734',
          nodeBorder: '#0092b8',
          clusterBkg: '#ffffff',
          clusterBorder: '#c8ced8',
          titleColor: '#0f1721',
          edgeLabelBackground: '#f6f8fa',
          fontFamily:
            "'Inter Variable', system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif",
        },
        themeCSS: `
          .node rect,
          .node polygon,
          .node circle,
          .node path {
            stroke-width: 1.5px !important;
          }
          .nodes > g:nth-of-type(4n + 1) rect,
          .nodes > g:nth-of-type(4n + 1) polygon { stroke: #0092b8 !important; fill: #e6f7fb !important; }
          .nodes > g:nth-of-type(4n + 2) rect,
          .nodes > g:nth-of-type(4n + 2) polygon { stroke: #1fa093 !important; fill: #e9f8f6 !important; }
          .nodes > g:nth-of-type(4n + 3) rect,
          .nodes > g:nth-of-type(4n + 3) polygon { stroke: #3b6ea5 !important; fill: #edf3fa !important; }
          .nodes > g:nth-of-type(4n + 4) rect,
          .nodes > g:nth-of-type(4n + 4) polygon { stroke: #556170 !important; fill: #f0f4f8 !important; }
          .nodeLabel,
          .nodeLabel p,
          .node .label {
            color: #0f1721 !important;
            fill: #0f1721 !important;
          }
          .edgeLabel,
          .edgeLabel p,
          .edgeLabel .label {
            color: #1e2734 !important;
            fill: #1e2734 !important;
            background-color: #f6f8fa !important;
          }
          .edgeLabel rect { fill: #f6f8fa !important; opacity: 0.94 !important; }
          .cluster rect {
            fill: #ffffff !important;
            stroke: #c8ced8 !important;
            stroke-dasharray: 4 3 !important;
          }
          .cluster-label,
          .cluster-label p,
          .cluster-label .nodeLabel,
          .cluster-label .nodeLabel p,
          .cluster text {
            color: #0f1721 !important;
            fill: #0f1721 !important;
            font-weight: 600 !important;
          }
        `,
      });
      return mermaid;
    });
  }
  return mermaidPromise;
}

/**
 * Extract the mermaid source from an expressive-code <pre>. Expressive-code
 * emits each source line as a separate <div class="ec-line"> with no text-node
 * newline between siblings; walk the per-line divs and rejoin with '\n'.
 */
function extractMermaidSource(pre) {
  const lines = pre.querySelectorAll('.ec-line');
  if (lines.length) {
    return Array.from(lines)
      .map((line) => line.textContent || '')
      .join('\n')
      .trim();
  }
  return (pre.textContent || '').trim();
}

/**
 * Strip intrinsic width/height so the SVG scales responsively via viewBox.
 */
function makeSvgResponsive(svg) {
  svg.removeAttribute('width');
  svg.removeAttribute('height');
  svg.style.width = '100%';
  svg.style.height = 'auto';
  svg.style.maxWidth = '100%';
  return svg;
}

let modalRoot = null;

function ensureModal() {
  if (modalRoot) return modalRoot;

  modalRoot = document.createElement('div');
  modalRoot.className = 'mermaid-modal';
  modalRoot.setAttribute('role', 'dialog');
  modalRoot.setAttribute('aria-modal', 'true');
  modalRoot.setAttribute('aria-label', 'Enlarged diagram');
  modalRoot.hidden = true;

  const surface = document.createElement('div');
  surface.className = 'mermaid-modal-surface';

  const closeBtn = document.createElement('button');
  closeBtn.type = 'button';
  closeBtn.className = 'mermaid-modal-close';
  closeBtn.setAttribute('aria-label', 'Close enlarged diagram');
  closeBtn.textContent = 'Close \u2715';

  const stage = document.createElement('div');
  stage.className = 'mermaid-modal-stage';

  surface.appendChild(closeBtn);
  surface.appendChild(stage);
  modalRoot.appendChild(surface);
  document.body.appendChild(modalRoot);

  let triggerElement = null;

  const close = () => {
    modalRoot.hidden = true;
    stage.innerHTML = '';
    document.body.style.overflow = '';
    if (triggerElement) {
      triggerElement.focus();
      triggerElement = null;
    }
  };

  closeBtn.addEventListener('click', close);
  modalRoot.addEventListener('click', (e) => {
    if (e.target === modalRoot) close();
  });
  document.addEventListener('keydown', (e) => {
    if (e.key === 'Escape' && !modalRoot.hidden) close();
  });

  modalRoot._open = (svgMarkup, triggerEl) => {
    triggerElement = triggerEl || document.activeElement;
    stage.innerHTML = svgMarkup;
    const svg = stage.querySelector('svg');
    if (svg) {
      svg.removeAttribute('width');
      svg.removeAttribute('height');
      svg.style.width = '100%';
      svg.style.height = '100%';
      svg.style.maxWidth = '100%';
      svg.style.maxHeight = '100%';
      svg.style.display = 'block';
    }
    modalRoot.hidden = false;
    document.body.style.overflow = 'hidden';
    closeBtn.focus();
  };

  return modalRoot;
}

async function renderMermaidBlocks() {
  const blocks = document.querySelectorAll('pre[data-language="mermaid"]');
  if (!blocks.length) return;

  let mermaid;
  try {
    mermaid = await loadMermaid();
  } catch (err) {
    console.warn('[mermaid-render] failed to load mermaid bundle', err);
    return;
  }

  for (const [i, pre] of blocks.entries()) {
    const src = extractMermaidSource(pre);
    if (!src) continue;

    // Take the whole Expressive Code wrapper, not just its <figure>: the
    // wrapper's stylesheet resets every descendant (`all: revert`), so a
    // diagram left inside it would lose the site's typography.
    const figure =
      pre.closest('.expressive-code') || pre.closest('figure') || pre;

    const container = document.createElement('div');
    container.className = 'mermaid-block not-content';

    const rendered = document.createElement('div');
    rendered.className = 'mermaid-rendered';

    const expandBtn = document.createElement('button');
    expandBtn.type = 'button';
    expandBtn.className = 'mermaid-expand';
    expandBtn.setAttribute('aria-label', 'Enlarge diagram');
    expandBtn.title = 'Enlarge diagram';
    expandBtn.innerHTML =
      '<svg viewBox="0 0 24 24" width="12" height="12" aria-hidden="true" fill="currentColor">' +
      '<path d="M4 4h6v2H6v4H4V4zm10 0h6v6h-2V6h-4V4zM4 14h2v4h4v2H4v-6zm14 0h2v6h-6v-2h4v-4z"/>' +
      '</svg>';

    const details = document.createElement('details');
    details.className = 'mermaid-source';
    const summary = document.createElement('summary');
    summary.textContent = 'Show mermaid source';
    details.appendChild(summary);

    figure.replaceWith(container);
    container.appendChild(rendered);
    rendered.appendChild(expandBtn);
    details.appendChild(figure);
    container.appendChild(details);

    try {
      const { svg, bindFunctions } = await mermaid.render(
        `lookout-mermaid-${i}-${Date.now()}`,
        src,
      );
      const svgHost = document.createElement('div');
      svgHost.className = 'mermaid-svg';
      svgHost.innerHTML = svg;
      rendered.insertBefore(svgHost, expandBtn);
      const svgEl = svgHost.querySelector('svg');
      if (svgEl) makeSvgResponsive(svgEl);
      if (bindFunctions) bindFunctions(svgHost);

      expandBtn.addEventListener('click', () => {
        // The inline diagram stays mounted behind the modal. Mermaid
        // prefixes its marker ids and scoped styles with the SVG's id, so
        // renaming that id in the copy keeps the two from colliding.
        const id = svgEl ? svgEl.id : '';
        const markup = id
          ? svgHost.innerHTML.split(id).join(`${id}-modal`)
          : svgHost.innerHTML;
        ensureModal()._open(markup, expandBtn);
      });
    } catch (err) {
      console.warn('[mermaid-render] failed to render diagram', err);
      container.replaceWith(figure);
    }
  }
}

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', renderMermaidBlocks);
} else {
  renderMermaidBlocks();
}
