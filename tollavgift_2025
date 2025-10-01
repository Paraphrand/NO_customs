library(jsonlite)
library(data.table)
library(stringr)

# --- helpers ---
as_num_comma <- function(x) {
  if (is.null(x) || identical(x, "")) return(NA_real_)
  x2 <- gsub(",", ".", as.character(x), fixed = TRUE)
  suppressWarnings(as.numeric(x2))
}
get_chr <- function(x, key) { v <- x[[key]]; if (is.null(v)) NA_character_ else as.character(v) }
get_hs  <- function(vare) {
  for (k in c("id","varenummer","varenr","commodityNumber","tariffnummer")) {
    v <- vare[[k]]
    if (!is.null(v)) return(as.character(v))
  }
  NA_character_
}

# --- load JSON ---
tl1_url <- "https://data.toll.no/dataset/f73e751a-8a12-48f1-89a1-f5c0a13d2475/resource/56619ffc-af57-4c41-aecd-aa5e568500ea/download/tollavgiftssats.json"
tl1_raw <- jsonlite::fromJSON(tl1_url, simplifyVector = FALSE)

varer <- tl1_raw$varer
stopifnot(!is.null(varer))

drop_sentinel <- TRUE   # remove 999999,99 placeholders

rows <- list()
row_i <- 1L

for (vare in varer) {
  hs <- get_hs(vare)
  if (is.na(hs)) next
  hs_clean <- gsub("[^0-9]", "", hs)

  avt <- vare$avtalesatser
  if (is.null(avt)) next

  for (avt_node in avt) {
    landgruppe <- get_chr(avt_node, "landgruppe")
    sats_list  <- avt_node$sats
    if (is.null(sats_list)) next

    for (s in sats_list) {
      val_raw   <- get_chr(s, "satsVerdi")
      unit_code <- get_chr(s, "satsEnhet")             # "P", "K", "S", etc.
      vf        <- get_chr(s, "fomdato")
      vt        <- get_chr(s, "tomdato")

      if (drop_sentinel && !is.na(val_raw) && trimws(val_raw) == "999999,99") next

      val_num <- as_num_comma(val_raw)

      rows[[row_i]] <- data.table::data.table(
        hs_code_national  = hs_clean,
        regime            = paste0("FTA:", landgruppe),
        duty_rate         = val_num,                   # always numeric
        rate_type         = if (!is.na(unit_code) && nzchar(unit_code)) unit_code else NA_character_,
        valid_from        = vf,
        valid_to          = vt,
        enhet             = get_chr(vare, "enhet"),
        enhet_beskrivelse = get_chr(vare, "enhetBeskrivelse")
      )
      row_i <- row_i + 1L
    }
  }
}

tl1_dt <- if (length(rows) > 0) data.table::rbindlist(rows, fill = TRUE) else data.table::data.table()

# clean and order
tl1_dt <- tl1_dt[nzchar(hs_code_national)]
data.table::setorder(tl1_dt, hs_code_national, regime, valid_from)

# preview
print(head(tl1_dt, 10))

# write
data.table::fwrite(tl1_dt, "norway_tl1_duty_rates_numeric.csv")
