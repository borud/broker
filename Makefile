ifeq ($(GOPATH),)
GOPATH := $(HOME)/go
endif

all: test_all lint vet staticcheck

test:
	@go test ./... -timeout 10s

test_clean:
	@go clean -testcache 
	@go test ./... -timeout 30s

test_cover:
	@go test ./... -cover -timeout 30s

test_race:
	@go test ./... -race -timeout 30s

test_all: test_cover test_race

lint:
	@revive -exclude  ./... 

vet:
	@go vet ./...

staticcheck:
	@staticcheck ./...

count:
	@gocloc .

# Here be dragons
init: dep-install

dep-install:
	@go get -u github.com/mgechev/revive \
		github.com/dominikh/go-tools/cmd/staticcheck \

