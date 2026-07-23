package main

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"

	"github.com/salesforce/converge/db"
	"github.com/salesforce/converge/internal/awsauth"
)

func runMigrate(cfg Config) error {
	ctx := context.Background()

	sqlDB, err := openMigrateDB(ctx, cfg)
	if err != nil {
		return err
	}
	defer sqlDB.Close()

	cmd := "up"
	if len(os.Args) > 2 {
		cmd = os.Args[2]
	}

	switch cmd {
	case "up":
		slog.Info("running migrations up")
		return db.Migrate(ctx, sqlDB)
	case "down":
		slog.Info("running migration down (one step)")
		return db.MigrateDown(ctx, sqlDB)
	case "reset":
		slog.Info("resetting all migrations")
		return db.MigrateReset(ctx, sqlDB)
	case "status":
		return db.MigrateStatus(ctx, sqlDB)
	default:
		return fmt.Errorf("unknown migrate command: %s (use up, down, reset, status)", cmd)
	}
}

// openMigrateDB builds the database/sql handle goose runs migrations through.
// With DB_IAM_AUTH off it is a plain sql.Open over the DSN. With it on, the
// connection goes through a pgx stdlib connector whose BeforeConnect mints an
// RDS IAM token per connect — the same hook the pool uses — so `converge
// migrate up` authenticates to Aurora with IAM exactly like the running pods.
func openMigrateDB(ctx context.Context, cfg Config) (*sql.DB, error) {
	if !cfg.DBIAMAuth {
		return sql.Open("pgx", cfg.DatabaseURL)
	}
	iam, err := awsauth.New(ctx)
	if err != nil {
		return nil, fmt.Errorf("init db iam auth for migrate: %w", err)
	}
	connCfg, err := pgx.ParseConfig(cfg.DatabaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url for migrate: %w", err)
	}
	connector := stdlib.GetConnector(*connCfg, stdlib.OptionBeforeConnect(iam.BeforeConnect()))
	return sql.OpenDB(connector), nil
}
