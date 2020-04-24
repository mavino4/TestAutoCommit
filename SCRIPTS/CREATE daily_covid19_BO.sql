CREATE TABLE "daily_covid19_BO" (
	"daily_id"	INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
	"daily_fecha"	TEXT NOT NULL,
	"daily_depto"	TEXT NOT NULL,
	"daily_delta_confirmados"	INTEGER NOT NULL DEFAULT -999999,
	"daily_delta_activos"	INTEGER NOT NULL DEFAULT -999999,
	"daily_delta_decesos"	INTEGER NOT NULL DEFAULT -999999,
	"daily_delta_recuperados"	INTEGER NOT NULL DEFAULT -999999,
	"daily_delta_sospechosos"	INTEGER NOT NULL DEFAULT -999999,
	"daily_delta_descartados"	INTEGER NOT NULL DEFAULT -999999,
	"daily_delta_total"	INTEGER NOT NULL DEFAULT -999999,
	"daily_total_confirmados"	INTEGER NOT NULL,
	"daily_total_activos"	INTEGER NOT NULL,
	"daily_total_decesos"	INTEGER NOT NULL,
	"daily_total_recuperados"	INTEGER NOT NULL,
	"daily_total_sospechosos"	INTEGER NOT NULL,
	"daily_total_descartados"	INTEGER NOT NULL,
	"daily_total_total"	INTEGER NOT NULL
);