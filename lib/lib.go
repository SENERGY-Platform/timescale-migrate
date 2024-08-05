/*
 * Copyright 2024 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package lib

import (
	"fmt"
	"log"

	"github.com/SENERGY-Platform/timescale-migrate/lib/config"
	"github.com/jackc/pgx"
)

type LibConfig struct {
	NumWorkers             int
	TimescaleSchema        string
	AdditionalSchemas      []string
	TimeFieldName          string
	SkipSchemaPreData      bool
	SkipSchemaPreDataDump  bool
	SkipSchemaPostData     bool
	SkipSchemaPostDataDump bool
	SkipRoles              bool
	SkipCreateHypertables  bool
	SkipTableData          bool
	SkipCAGG               bool
	SkipPolicies           bool
	SkipStatistics         bool
}

type Lib struct {
	sourceConfig      config.Config
	targetConfig      config.Config
	libConfig         LibConfig
	sourceDb          *pgx.ConnPool
	targetDb          *pgx.ConnPool
	sourcePsqlConnStr string
	targetPsqlConnStr string
}

func New(source config.Config, target config.Config, libConfig LibConfig) (*Lib, error) {
	log.Println("Connecting to source PSQL...")
	// open database
	sourceDb, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     source.PostgresHost,
			Port:     uint16(source.PostgresPort),
			Database: source.PostgresDb,
			User:     source.PostgresUser,
			Password: source.PostgresPw,
		},
		MaxConnections: libConfig.NumWorkers + 2})
	if err != nil {
		return nil, err
	}

	log.Println("Connecting to target PSQL...")
	// open database
	targetDb, err := pgx.NewConnPool(pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     target.PostgresHost,
			Port:     uint16(target.PostgresPort),
			Database: target.PostgresDb,
			User:     target.PostgresUser,
			Password: target.PostgresPw,
		},
		MaxConnections: libConfig.NumWorkers + 2})
	if err != nil {
		return nil, err
	}

	sourcePsqlConnStr := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		source.PostgresUser, source.PostgresPw, source.PostgresHost, source.PostgresPort, source.PostgresDb)
	targetPsqlConnStr := fmt.Sprintf("postgresql://%s:%s@%s:%d/%s?sslmode=disable",
		target.PostgresUser, target.PostgresPw, target.PostgresHost, target.PostgresPort, target.PostgresDb)

	return &Lib{
		sourceConfig:      source,
		targetConfig:      target,
		libConfig:         libConfig,
		sourceDb:          sourceDb,
		targetDb:          targetDb,
		sourcePsqlConnStr: sourcePsqlConnStr,
		targetPsqlConnStr: targetPsqlConnStr,
	}, nil
}

func (this *Lib) Run() (err error) {
	if !this.libConfig.SkipRoles {
		err = this.migrateRoles()
		if err != nil {
			return err
		}
	}
	if !this.libConfig.SkipSchemaPreData {
		err = this.migrateSchemaPreData(this.libConfig.SkipSchemaPreDataDump)
		if err != nil {
			return err
		}
	}
	if !this.libConfig.SkipCreateHypertables {
		err = this.createHypertables()
		if err != nil {
			return err
		}
	}
	if !this.libConfig.SkipTableData {
		this.copyTableData()
	}
	if !this.libConfig.SkipSchemaPostData {
		err = this.migrateSchemaPostData(this.libConfig.SkipSchemaPostDataDump)
		if err != nil {
			return err
		}
	}
	if !this.libConfig.SkipCAGG {
		err = this.recreateCAGG()
		if err != nil {
			return err
		}
	}
	if !this.libConfig.SkipPolicies {
		err = this.migratePolicies()
		if err != nil {
			return err
		}
	}

	if !this.libConfig.SkipStatistics {
		err = this.updateStats()
		if err != nil {
			return err
		}
	}
	return nil
}
