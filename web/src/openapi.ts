import { createApiReference } from '@scalar/api-reference';
import '@scalar/api-reference/style.css';

const target = document.getElementById('app');

if (!target) {
  throw new Error('OpenAPI mount target is missing');
}

createApiReference(target, {
  url: '/openapi.json',
  darkMode: true,
  showSidebar: true,
  hideClientButton: true,
  agent: {
    disabled: true
  },
  mcp: {
    disabled: true
  },
  hiddenClients: true,
  operationTitleSource: 'path',
  customCss: `
    a[href="https://www.scalar.com"] {
      display: none !important;
    }
  `,
  withDefaultFonts: false,
  defaultOpenFirstTag: true
});
