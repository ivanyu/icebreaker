import { defineConfig } from '@hey-api/openapi-ts';

export default defineConfig({
  input: 'https://raw.githubusercontent.com/apache/iceberg/refs/heads/main/open-api/rest-catalog-open-api.yaml',
  output: 'src/rest_catalog_client',
  plugins: ['@hey-api/client-fetch'],
});
