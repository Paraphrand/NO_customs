# ------------------ Innførselsavgift (import fees) → tidy numeric CSV ------------------
# Output: norway_import_fees_numeric.csv
# --------------------------------------------------------------------------------------

library(jsonlite)
library(data.table)
library(stringr)

# Helpers
as_num_comma <- function(x) {
  if (base::is.null(x) || base::identical(x, "")) return(NA_real_)
  base::suppressWarnings(base::as.numeric(base::gsub(",", ".", base::as.character(x), fixed = TRUE)))
}
get_chr <- function(x, key) { v <- x[[key]]; if (base::is.null(v)) NA_character_ else base::as.character(v) }
get_hs  <- function(vare) {
  v <- vare[["id"]]
  if (!base::is.null(v)) return(base::as.character(v))
  v <- vare[["varenummer"]]
  if (!base::is.null(v)) return(base::as.character(v))
  NA_character_
}
as_logical_str <- function(x) {
  # converts "true"/"false"/TRUE/FALSE to logical; returns NA for anything else
  if (base::is.null(x)) return(NA)
  if (is.logical(x)) return(x)
  xs <- tolower(base::as.character(x))
  out <- rep(NA, length(xs))
  out[xs == "true"]  <- TRUE
  out[xs == "false"] <- FALSE
  out
}

# Load JSON (use your cached file if you have it)
fees_url <- "https://data.toll.no/dataset/ff43ccfa-2686-48da-a216-02d627f4937a/resource/64c9e796-2dfb-4e77-8201-78ed3fbfb899/download/innfoerselsavgift.json"
raw_path <- "innfoerselsavgift.json"
if (!base::file.exists(raw_path)) utils::download.file(fees_url, raw_path, mode = "wb")

fees_raw <- jsonlite::fromJSON(raw_path, simplifyVector = FALSE)
if (base::is.null(fees_raw$varer) || !base::is.list(fees_raw$varer)) {
  stop("Expected top-level 'varer' list in the JSON.")
}

varer <- fees_raw$varer
n_varer <- length(varer)
out_chunks <- vector("list", n_varer)
pb <- utils::txtProgressBar(min = 0, max = n_varer, style = 3)

for (i in seq_len(n_varer)) {
  vare <- varer[[i]]

  hs_raw <- get_hs(vare)
  if (is.na(hs_raw)) { out_chunks[[i]] <- NULL; utils::setTxtProgressBar(pb, i); next }
  hs_code <- gsub("[^0-9]", "", hs_raw)

  avgiftsatser <- vare[["avgiftsatser"]]
  if (is.null(avgiftsatser) || !is.list(avgiftsatser) || length(avgiftsatser) == 0L) {
    out_chunks[[i]] <- NULL; utils::setTxtProgressBar(pb, i); next
  }

  # build rows for this vare
  per_vare <- list()
  ai <- 1L

  for (as_node in avgiftsatser) {
    landgruppe     <- get_chr(as_node, "landgruppe")
    landgruppenavn <- get_chr(as_node, "landgruppenavn")
    avgiftstyper   <- as_node[["avgiftstyper"]]
    if (is.null(avgiftstyper) || !is.list(avgiftstyper) || length(avgiftstyper) == 0L) next

    for (typ in avgiftstyper) {
      avgiftstype            <- get_chr(typ, "avgiftstype")
      avgiftstypebeskrivelse <- get_chr(typ, "avgiftstypebeskrivelse")
      avgiftsgrupper         <- typ[["avgiftsgrupper"]]
      if (is.null(avgiftsgrupper) || !is.list(avgiftsgrupper) || length(avgiftsgrupper) == 0L) next

      # Each element inside avgiftsgrupper is a leaf with the fields we need
      m <- length(avgiftsgrupper)
      if (m == 0L) next

      # Pre-allocate vectors
      avgiftsgruppe            <- integer(m)
      avgiftsgruppebeskrivelse <- character(m)
      sats                     <- character(m)
      oresats                  <- logical(m); oresats[] <- NA
      enhet                    <- character(m)
      enhetbeskrivelse         <- character(m)
      vf                       <- character(m)
      vt                       <- character(m)

      for (k in seq_len(m)) {
        g <- avgiftsgrupper[[k]]
        avgiftsgruppe[k]            <- as.integer(g[["avgiftsgruppe"]])
        avgiftsgruppebeskrivelse[k] <- get_chr(g, "avgiftsgruppebeskrivelse")
        sats[k]                     <- get_chr(g, "sats")
        oresats[k]                  <- as_logical_str(g[["oresats"]])
        enhet[k]                    <- get_chr(g, "enhet")              # "P" etc.
        enhetbeskrivelse[k]         <- get_chr(g, "enhetbeskrivelse")   # "Prosent av verdien"
        vf[k]                       <- get_chr(g, "fomdato")
        vt[k]                       <- get_chr(g, "tomdato")
      }

      # Numeric duty; apply øre→kroner if oresats is TRUE
      duty_val <- vapply(sats, as_num_comma, 0.0)
      duty_val <- ifelse(isTRUE(oresats), duty_val / 100, duty_val)

      per_vare[[ai]] <- data.table::data.table(
        hs_code_national          = hs_code,
        landgruppe                = landgruppe,
        landgruppenavn            = landgruppenavn,
        avgiftstype               = avgiftstype,
        avgiftstypebeskrivelse    = avgiftstypebeskrivelse,
        avgiftsgruppe             = avgiftsgruppe,
        avgiftsgruppebeskrivelse  = avgiftsgruppebeskrivelse,
        duty_value                = duty_val,             # numeric only
        rate_type                 = ifelse(nzchar(enhet), enhet, NA_character_),
        valid_from                = vf,
        valid_to                  = vt,
        enhet                     = enhet,
        enhet_beskrivelse         = enhetbeskrivelse,
        oresats_flag              = ifelse(is.na(oresats), NA, oresats)
      )
      ai <- ai + 1L
    } # end typ
  } # end as_node

  out_chunks[[i]] <- if (ai > 1L) data.table::rbindlist(per_vare, use.names = TRUE, fill = TRUE) else NULL
  utils::setTxtProgressBar(pb, i)
}
close(pb)

fees_dt <- data.table::rbindlist(out_chunks, use.names = TRUE, fill = TRUE)

# Basic cleaning + order
if (nrow(fees_dt) == 0L) {
  stop("No rows extracted. Check the JSON keys again; this script expects id/avgiftsatser/avgiftstyper/avgiftsgrupper.")
}

fees_dt <- fees_dt[nzchar(hs_code_national)]
data.table::setorder(fees_dt, hs_code_national, landgruppe, avgiftstype, avgiftsgruppe, valid_from)

# Write CSV
data.table::fwrite(fees_dt, "norway_import_fees_numeric.csv")
base::message("Rows written: ", nrow(fees_dt))
