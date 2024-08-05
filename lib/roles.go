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
	"log"
	"os"
	"os/exec"
)

func (l *Lib) migrateRoles() error {
	log.Println("migrate roles")

	log.Println("dumping roles from source")
	cmd := "pg_dumpall -d " + l.sourcePsqlConnStr + " -l " + l.sourceConfig.PostgresDb + " --quote-all-identifiers --roles-only --file=roles.sql"
	ex := exec.Command("bash", "-c", cmd)
	output, err := ex.Output()
	if err != nil {
		return err
	}
	if len(output) > 0 {
		log.Println(string(output))
	}

	log.Println("restoring roles to target")
	cmd = "psql " + l.targetPsqlConnStr + " --file=roles.sql"
	ex = exec.Command("bash", "-c", cmd)
	output, err = ex.Output()
	if err != nil {
		return err
	}
	if len(output) > 0 {
		log.Println(string(output))
	}

	_ = os.Remove("roles.sql")

	return nil
}
