package main

import (
	"sort"
	"strconv"
	"strings"
)

func dbCheck() bool {
	DBSPlain = make(map[int]string)

	var plainlist []int

	if dbparam != nil && strings.TrimSpace(*dbparam) != "" {

		dbparse := strings.Split(*dbparam, ",")

		ldbs := len(dbparse)
		if ldbs < 1 || ldbs > maxDB {
			log.Errorf("databases count wrong: %d\n", ldbs)
			return false
		}

		DBS = make([]int, ldbs)
		for i, v := range dbparse {
			dbi, err := strconv.Atoi(v)
			DBS[i] = dbi
			if err != nil {
				log.Error("Error parsing databases ", err)
				return false
			}
		}
		dbcopy := append([]int(nil), DBS...)

		sort.Ints(dbcopy)

		dbcheck := dbcopy[0]
		for i := 1; i < ldbs; i++ {
			nextdb := dbcopy[i]

			if nextdb == dbcheck {
				log.Error("Database dublicated: %d\n", dbcheck)
				return false
			}
			dbcheck = nextdb
		}

		if dbplain != nil && strings.TrimSpace(*dbplain) != "" {

			dbparse := strings.Split(*dbplain, ",")

			ldbp := len(dbparse)
			if ldbp < 1 || ldbp > maxDB {
				log.Errorf("databases count wrong: %d\n", ldbp)
				return false
			}

			if ldbp > ldbs {
				log.Error("Plain database exceeds all database list")
				return false
			}

			plainlist = make([]int, ldbp)
			for ip, vp := range dbparse {
				dbi, err := strconv.Atoi(vp)
				DBSPlain[dbi] = "+"

				plainlist[ip] = dbi

				if err != nil {
					log.Error("Error parsing databases ", err)
					return false
				}

			}
			sort.Ints(plainlist)
			dbcheckplain := plainlist[0]

			for i := 1; i < ldbp; i++ {
				nextdb := plainlist[i]

				if nextdb == dbcheckplain {
					log.Error("Database dublicated: %d\n", dbcheck)
					return false
				}
				dbcheck = nextdb
			}
			//	log.Info("Databases: ", DBSPlain)
			log.Info("Plain databases in list: ", plainlist)

		} else {
			log.Warning("Plain databases not defined")
			//return false
		}

		log.Info("Databases: ", DBS)
		log.Info("Databases in list: ", dbcopy)

		for _, iv := range plainlist {
			ivp := sort.SearchInts(dbcopy, iv)
			if ivp >= ldbs || dbcopy[ivp] != iv {
				log.Error("Plain database not present in db list")
				return false
			}

		}
	} else {
		log.Error("No databases defined")
		return false
	}

	return true
}
