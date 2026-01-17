# Database migrations (local)
migrate-up:
	@echo "⬆️  Running migrations (local)..."
	migrate -path db/migrations -database "$(DB_LOCAL)" up

migrate-down:
	@echo "⬇️  Rolling back last migration (local)..."
	migrate -path db/migrations -database "$(DB_LOCAL)" down 1

migrate-status:
	@echo "📊 Migration status (local):"
	migrate -path db/migrations -database "$(DB_LOCAL)" version

migrate-create:
	@read -p "Migration name: " name; \
	migrate create -ext sql -dir db/migrations -seq $$name
