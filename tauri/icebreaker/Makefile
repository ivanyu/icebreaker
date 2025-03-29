.PHONY: rest_catalog_client
rest_catalog_client:
	npx @hey-api/openapi-ts \
		-i https://raw.githubusercontent.com/apache/iceberg/refs/heads/main/open-api/rest-catalog-open-api.yaml \
		-o src/rest_catalog_client \
		-c @hey-api/client-fetch
