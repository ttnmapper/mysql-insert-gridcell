package main

import (
	"github.com/j4/gosm"
	"github.com/jmoiron/sqlx"
	"log"
	"time"
	"ttnmapper-mysql-insert-gridcell/types"
)

var queueForUpdateChannel = make(chan types.TtnMapperUplinkMessage)

func processQueueForUpdate() {

	for {
		message := <-queueForUpdateChannel
		//log.Printf("   [TMS Queue] Adding to redraw tile queue")

		for i := 1; i <= 19; i++ {
			tile := gosm.NewTileWithLatLong(message.TtnMLatitude, message.TtnMLongitude, i)

			updateTileEntry(tile.X-1, tile.Y-1, tile.Z)
			updateTileEntry(tile.X-1, tile.Y, tile.Z)
			updateTileEntry(tile.X-1, tile.Y+1, tile.Z)

			updateTileEntry(tile.X, tile.Y-1, tile.Z)
			updateTileEntry(tile.X, tile.Y, tile.Z)
			updateTileEntry(tile.X, tile.Y+1, tile.Z)

			updateTileEntry(tile.X+1, tile.Y-1, tile.Z)
			updateTileEntry(tile.X+1, tile.Y, tile.Z)
			updateTileEntry(tile.X+1, tile.Y+1, tile.Z)

		}
		log.Printf("   [TMS Queue] Added to redraw tile queue")
	}
}

func updateTileEntry(x int, y int, z int) {

	db, err := sqlx.Open("mysql", myConfiguration.MysqlUser+":"+myConfiguration.MysqlPassword+"@tcp("+myConfiguration.MysqlHost+":"+myConfiguration.MysqlPort+")/"+myConfiguration.MysqlDatabase+"?parseTime=true")
	if err != nil {
		panic(err.Error())
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()

	stmtSelect, err := db.PrepareNamed("SELECT * FROM tiles_to_redraw WHERE x = :x AND y = :y AND z = :z")
	if err != nil {
		panic(err.Error())
	}
	defer stmtSelect.Close()

	stmtInsert, err := db.PrepareNamed("INSERT INTO tiles_to_redraw " +
		"(x, y, z, last_queued) " +
		"VALUES " +
		"(:x, :y, :z, :last_queued)")
	if err != nil {
		panic(err.Error())
	}
	defer stmtInsert.Close()

	stmtUpdate, err := db.PrepareNamed("UPDATE tiles_to_redraw SET " +
		"last_queued = :last_queued " +
		"WHERE x = :x AND y = :y AND z = :z")
	if err != nil {
		panic(err.Error())
	}
	defer stmtUpdate.Close()

	m := map[string]interface{}{"x": x, "y": y, "z": z}
	var entry = types.MysqlTileToRedraw{}

	result := stmtSelect.QueryRow(m)
	err = result.StructScan(&entry)
	if err != nil {
		//log.Print("[TMS Queue] " + err.Error())
	}

	if entry.X != z || entry.Y != y || entry.Z != z {

		entry.X = x
		entry.Y = y
		entry.Z = z
		entry.LastQueued = time.Now()

		_, err := stmtInsert.Exec(entry)
		if err != nil {
			log.Print(err.Error())
		}

		//lastInsertId, err := result.LastInsertId()
		//if err != nil {
		//	log.Print(err.Error())
		//}
		//log.Printf("[TMS Queue] Inserted row id=%d", lastInsertId)

	} else {

		entry.LastQueued = time.Now()

		_, err := stmtUpdate.Exec(entry)
		if err != nil {
			log.Print(err.Error())
		}

		//rowsAffected, err := result.RowsAffected()
		//if err != nil {
		//	log.Print(err.Error())
		//}
		//log.Printf("[TMS Queue] Updated %d rows", rowsAffected)

	}
}
