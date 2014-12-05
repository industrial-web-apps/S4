REPORTER = spec

test:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \

test-cov:
	@NODE_ENV=test ./node_modules/.bin/istanbul \
		cover _mocha -- -R $(REPORTER)

test-w:
	@NODE_ENV=test ./node_modules/.bin/mocha \
		--reporter $(REPORTER) \
		--growl \
		--watch

.PHONY: test test-w
