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

package main

import (
	"flag"
	"strings"

	"github.com/SENERGY-Platform/timescale-migrate/lib"
	"github.com/SENERGY-Platform/timescale-migrate/lib/config"

	"log"
)

func main() {
	sourceLocation := flag.String("source", "source.json", "source configuration file")
	targetLocation := flag.String("target", "target.json", "target configuration file")
	numWorkers := flag.Int("num-workers", 4, "number of parallel workers")
	timescaleSchema := flag.String("timescale-schema", "public", "schema where hypertables are located in")
	additionalSchemas := flag.String("additional-schemas", "rules,tableworker,usage", "comma-separated list of additional non-timescale schemas which should be cloned")
	timeFieldName := flag.String("time-field", "time", "")
	skipSchemaPreData := flag.Bool("skip-schema-pre-data", false, "skip the schema creation pre data")
	skipSchemaPreDataDump := flag.Bool("skip-schema-pre-data-dump", false, "skips the dumping of the schema pre-data, needs the dump file to be present in current wd")
	skipSchemaPostData := flag.Bool("skip-schema-post-data", false, "skip the schema creation post data")
	skipSchemaPostDataDump := flag.Bool("skip-schema-post-data-dump", false, "skips the dumping of the schema post-data, needs the dump file to be present in current wd")
	skipRoles := flag.Bool("skip-roles", false, "skip the migration of roles")
	skipCreateHypertables := flag.Bool("skip-create-hypertables", false, "skips the creation of hypertables")
	skipTableData := flag.Bool("skip-table-data", false, "skips the table data copy")
	skipCAGG := flag.Bool("skip-cagg", false, "skips the creation of continous aggregates")
	skipPolicies := flag.Bool("skip-policies", false, "skips the re-creation of policies")
	skipStatistics := flag.Bool("skip-statistics", false, "skips the the calculation of table statistics")

	flag.Parse()

	source, err := config.Load(*sourceLocation)
	if err != nil {
		log.Fatal("ERROR: unable to load source config", err)
	}

	target, err := config.Load(*targetLocation)
	if err != nil {
		log.Fatal("ERROR: unable to load target config", err)
	}

	libConfig := lib.LibConfig{
		NumWorkers:             *numWorkers,
		TimescaleSchema:        *timescaleSchema,
		AdditionalSchemas:      strings.Split(*additionalSchemas, ","),
		TimeFieldName:          *timeFieldName,
		SkipSchemaPreData:      *skipSchemaPreData,
		SkipSchemaPreDataDump:  *skipSchemaPreDataDump,
		SkipSchemaPostData:     *skipSchemaPostData,
		SkipSchemaPostDataDump: *skipSchemaPostDataDump,
		SkipRoles:              *skipRoles,
		SkipCreateHypertables:  *skipCreateHypertables,
		SkipTableData:          *skipTableData,
		SkipCAGG:               *skipCAGG,
		SkipPolicies:           *skipPolicies,
		SkipStatistics:         *skipStatistics,
	}

	l, err := lib.New(source, target, libConfig)
	if err != nil {
		log.Fatal("ERROR: ", err)
	}
	err = l.Run()
	if err != nil {
		log.Fatal("ERROR: ", err)
	}
}
