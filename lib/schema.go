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
	"errors"
	"log"
	"os"
	"os/exec"
	"strconv"
)

func (l *Lib) migrateSchemaPreData(skipDump bool) error {
	return l.migrateSchema("pre-data", skipDump)
}

func (l *Lib) migrateSchemaPostData(skipDump bool) error {
	return l.migrateSchema("post-data", skipDump)
}

func (l *Lib) migrateSchema(section string, skipDump bool) error {
	log.Println("migrate schema", section)

	if !skipDump {
		log.Println("dumping schema " + section + ", this might take a while...")
		cmd := "pg_dump " + l.sourcePsqlConnStr + " -Fc -v --section=" + section + " --exclude-schema=\"_timescaledb*\" -f dump_" + section + ".dump"
		ex := exec.Command("sh", "-c", cmd)
		output, err := ex.Output()
		if err != nil {
			return err
		}
		if len(output) > 0 {
			log.Println(string(output))
		}
	}

	log.Println("restoring schema", section)
	exists, err := exists("dump_" + section + ".dump")
	if err != nil {
		return err
	}
	if !exists {
		return errors.New("dump file does not exists, did you skip the creation?")
	}

	cmd := "pg_restore -d " + l.targetPsqlConnStr + " --no-owner -j " + strconv.Itoa(l.libConfig.NumWorkers) + " -Fc -v dump_" + section + ".dump"
	ex := exec.Command("sh", "-c", cmd)
	output, err := ex.Output()
	if err != nil {
		exitErr, ok := err.(*exec.ExitError)
		if ok {
			log.Println("Warning: Restore finished with errors, this might be ok though\n", string(exitErr.Stderr))
		} else {
			return err
		}
	}
	if len(output) > 0 {
		log.Println(string(output))
	}

	return nil
}

func exists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}
