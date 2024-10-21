#
## Extract data ----
#
#dvw_files <- list.files("VM_Data")
#
#for (file in dvw_files) {
#
#  print(file)
#
#  data <- try(
#    datavolley::dv_read(glue::glue("VM_Data/{file}"), insert_technical_timeouts = FALSE)
#  )
#
#  if ("datavolley" %in% class(data)) {
#    plays <- datavolley::plays(data)
#    out_file <- stringr::str_replace_all(file, "dvw", "csv")
#    data.table::fwrite(plays, file = glue::glue("R/data_datavolley/{out_file}"))
#  }
#}
#
#csv_files <- list.files("R/data_datavolley/")
#plays <- plyr::ldply(glue::glue("R/data_datavolley/{csv_files}"), data.table::fread)
#data.table::fwrite(plays, file = "R/data_datavolley.csv")
#

# Load data ----

meta_data <- datavolley::dv_read(
  glue::glue("VM_Data/{list.files('VM_Data')[1]}"),
  insert_technical_timeouts = TRUE
)$meta

plays <- data.table::fread("R/data/datavolley.csv",
  select = c("match_id", "set_number", "point_id", "home_team_score", "visiting_team_score", "team_touch_id",
    "team", "serving_team", "point", "skill", "evaluation_code",
    "team_id", "home_team_id", "visiting_team_id", "player_id", "player_name", "start_zone",
    "end_zone", "start_coordinate_x", "start_coordinate_y", "end_coordinate_x", "end_coordinate_y",
    "attack_code",
    "home_setter_position", "visiting_setter_position",
    "home_player_id1", "home_player_id2", "home_player_id3", "home_player_id4", "home_player_id5", "home_player_id6",
    "home_p1", "home_p2", "home_p3", "home_p4", "home_p5", "home_p6",
    "visiting_p1", "visiting_p2", "visiting_p3", "visiting_p4", "visiting_p5", "visiting_p6",
    "visiting_player_id1", "visiting_player_id2", "visiting_player_id3",
    "visiting_player_id4", "visiting_player_id5", "visiting_player_id6",
    "code"
  ),
#  nrows = 1000797,
  na.strings = ""
) |>
  tibble::as_tibble()

team <- read.csv("R/data/team.csv", stringsAsFactors = FALSE)
conference <- read.csv("R/data/conference.csv", stringsAsFactors = FALSE)

team_conf <- plays |>
  dplyr::distinct(match_id, team_id_home = home_team_id, team_id_away = visiting_team_id) |>
  dplyr::left_join(team, by = c("team_id_home" = "team_id")) |>
  dplyr::rename(conf_id_home = conference_id) |>
  dplyr::left_join(team, by = c("team_id_away" = "team_id")) |>
  dplyr::rename(conf_id_away = conference_id) |>
  dplyr::select(match_id, team_id_home, team_id_away, conf_id_home, conf_id_away)

player <- plays |>
  dplyr::filter(!is.na(player_id), player_id != "unknown player") |>
  dplyr::count(player_id, player_name, team_id) |>
  dplyr::arrange(-n) |>
  dplyr::group_by(player_id) |>
  dplyr::slice(1) |>
  dplyr::ungroup() |>
  dplyr::select(player_id, player_name, team_id)

team_lineup_away <- plays |>
  dplyr::select(match_id, set_number, team_id = visiting_team_id, dplyr::starts_with("visiting_player_id")) |>
  dplyr::distinct() |>
  tidyr::pivot_longer(cols = dplyr::starts_with("visiting_player_id"), values_to = "player_id") |>
  dplyr::distinct(match_id, set_number, team_id, player_id) |>
  dplyr::filter(player_id != "unkown player")

team_lineup_home <- plays |>
  dplyr::select(match_id, set_number, team_id = home_team_id, dplyr::starts_with("home_player_id")) |>
  dplyr::distinct() |>
  tidyr::pivot_longer(cols = dplyr::starts_with("home_player_id"), values_to = "player_id") |>
  dplyr::distinct(match_id, set_number, team_id, player_id) |>
  dplyr::filter(player_id != "unkown player")

team_lineup <- dplyr::bind_rows(team_lineup_away, team_lineup_home)

libero <- plays |>
  dplyr::filter(skill %in% c("Serve", "Reception", "Dig")) |>
  dplyr::count(match_id, set_number, team_id, player_id) |>
  dplyr::anti_join(team_lineup, by = c("match_id", "set_number", "team_id", "player_id")) |>
  dplyr::group_by(match_id, set_number, team_id) |>
  dplyr::arrange(-n) |>
  dplyr::slice(1) |>
  dplyr::ungroup() |>
  dplyr::select(match_id, set_number, team_id, libero_id = player_id)


# Estimate the Markov chain ----

# From https://stackoverflow.com/questions/24862046/
cumpaste <- function(x, .sep = " ") {
  concat <- paste(x, collapse = .sep)
  return(substring(concat, 1L, cumsum(c(nchar(x[[1L]]), nchar(x[-1L]) + nchar(.sep)))))
}

contact <- plays |>
  dplyr::left_join(team_conf, by = "match_id") |>
  dplyr::mutate(
    team_id_offense = team_id,
    team_id_defense = ifelse(team_id_offense == team_id_home, team_id_away, team_id_home),
    conf_id_offense = ifelse(team_id_offense == team_id_home, conf_id_home, conf_id_away),
    conf_id_defense = ifelse(team_id_offense == team_id_home, conf_id_away, conf_id_home),
    serve_receive = ifelse(team == serving_team, "S", "R"),
    is_volley_end = dplyr::coalesce(
      dplyr::lead(point, 1) | (team_touch_id != dplyr::lead(team_touch_id, 1)),
      TRUE
    ),
    abbrev = dplyr::case_when(
      point ~ "P",
      is.na(skill) ~ NA,
      grepl("Unknown", skill) ~ NA,
      skill == "Serve" ~ "SV",
      skill == "Attack" ~ paste0("A", attack_code),
      TRUE ~ paste0(substring(skill, 1, 1), evaluation_code)
    ),
    player_id_lead_1 = dplyr::lead(player_id, 1),
    skill_lead_1 = dplyr::lead(skill, 1),
    evaluation_code_lead_1 = dplyr::lead(evaluation_code, 1),
  ) |>
  dplyr::filter(!is.na(abbrev), !is.na(serving_team)) |>
  dplyr::group_by(match_id, point_id, team_touch_id, point) |>
  # STEP 0: DEFINE THE STATE OF THE MARKOV CHAIN
  dplyr::mutate(
    state = paste0(serve_receive[1], "_", cumpaste(abbrev, .sep = ".")),
    num_contacts = length(abbrev)   # much faster than counting number of periods in state
  ) |>
  dplyr::ungroup()

# Data quality checks: can't have more than four touches (including block), and
# serves and attacks must end the volley (may want to relax this assumption for attacks).
# For any points where this is violated, we want to throw out the whole point.
bad_data <- contact |>
  dplyr::group_by(state) |>
  dplyr::mutate(n = dplyr::n()) |>
  dplyr::ungroup() |>
  dplyr::filter(
    (num_contacts > 4) |
    (abbrev %in% c("SV", "A") & !is_volley_end) |
    (n < 100)
  ) |>
  dplyr::distinct(match_id, point_id)

# STEP 1: COUNT THE NUMBER OF TRANSITIONS BETWEEN EACH PAIR OF STATES
state_transition <- contact |>
  dplyr::anti_join(bad_data, by = c("match_id", "point_id")) |>
  dplyr::count(state, next_state = dplyr::lead(state, 1)) |>
  # Force points to be terminating states
  dplyr::mutate(
    next_state = dplyr::case_when(
      state == "R_P" ~ "R_P",
      state == "S_P" ~ "S_P",
      TRUE ~ next_state
    )
  )

# What is the range of sample sizes?
state_transition |>
  dplyr::group_by(state) |>
  dplyr::summarize(n = sum(n), .groups = "drop") |>
  dplyr::arrange(n) |>
  head()
state_transition |>
  dplyr::group_by(state) |>
  dplyr::summarize(n = sum(n), .groups = "drop") |>
  dplyr::arrange(-n) |>
  head()


# STEP 2: CONVERT TRANSITION COUNTS INTO TRANSITION PROBABILITIES
state_transition_wide <- state_transition |>
  # Add a row so that S_SV appears once in the next_state column (for creating transition matrix)
  dplyr::bind_rows(tibble::tibble(state = "S_SV", next_state = "S_SV", n = 0)) |>
  # Make sure we don't have any repeated state-next state pairs
  dplyr::group_by(state, next_state) |>
  dplyr::summarize(n = sum(n), .groups = "drop") |>
  dplyr::group_by(state) |>
  dplyr::transmute(state, next_state, prob = n / sum(n)) |>
  dplyr::arrange(next_state) |>
  tidyr::pivot_wider(names_from = next_state, values_from = prob, values_fill = 0) |>
  dplyr::arrange(state)

# Step 2.5: Convert dataframe to matrix for multiplication
state_transition_matrix <- state_transition_wide |>
  tibble::column_to_rownames("state") |>
  as.matrix()

state_transition_matrix_limit <- state_transition_matrix

# STEP 3: MULTIPLY THE TRANSITION MATRIX BY ITSELF A BUNCH OF TIMES
for (i in 1:100) {
  state_transition_matrix_limit <- state_transition_matrix_limit %*% state_transition_matrix
}

# STEP 4: EXTRACT TERMINAL STATE PROBABILITIES FOR EACH STARTING STATE
sideout_prob <- state_transition_matrix_limit[, c("S_P", "R_P")] |>
  tibble::as_tibble() |>
  tibble::add_column(state = rownames(state_transition_matrix), .before = 1) |>
  dplyr::transmute(
    state,
    sideout_prob = R_P
  )

win_prob_from_dig <- sideout_prob |>
  dplyr::filter(nchar(state) == 4, substring(state, 1, 3) == "R_D") |>
  dplyr::transmute(eval = substring(state, 4), win_prob = sideout_prob)

contact_sideout_prob <- contact |>
  dplyr::left_join(sideout_prob, by = "state") |>
  dplyr::mutate(sideout_prob_lead_1 = dplyr::lead(sideout_prob, 1))

# Distribute credit/debit to individual players ----

serve_data <- contact_sideout_prob |>
  dplyr::filter(
    skill == "Serve",
    skill_lead_1 == "Reception" | evaluation_code == "=",
    !is.na(sideout_prob_lead_1),
    player_id != "unknown player",
    !is.na(conf_id_home),
    !is.na(conf_id_away),
    !is.na(team_id_home),
    !is.na(team_id_home)
  ) |>
  dplyr::mutate(
    server_id = player_id,
    receiver_id = player_id_lead_1,
    is_error = evaluation_code == "=",
    no_error = !is_error,
    exp_no_error = weighted.mean(sideout_prob_lead_1, w = !is_error),
    pg_is_error = ifelse(is_error, 1, exp_no_error) - sideout_prob,
    pg_no_error = ifelse(is_error, 0, sideout_prob_lead_1 - exp_no_error)
  ) |>
  dplyr::group_by(server_id) |>
  dplyr::mutate(count = length(server_id)) |>
  dplyr::ungroup() |>
  dplyr::mutate(server_id_model = ifelse(count < 100, "0", server_id)) |>
  dplyr::group_by(receiver_id) |>
  dplyr::mutate(count = length(receiver_id)) |>
  dplyr::ungroup() |>
  dplyr::mutate(receiver_id_model = ifelse(count < 100, "0", receiver_id))

#stop('modeling starts here')

# What is our sample size?
 serve_data |>
  dplyr::summarize(sum(no_error))

.time <- Sys.time()
serve_model <- serve_data |>
  dplyr::filter(no_error) |>
  with(
    lme4::lmer(
      pg_no_error ~ (1 | conf_id_offense) + (1 | conf_id_defense) + (1 | team_id_offense) + (1 | team_id_defense) + (1 | server_id_model) + (1 | receiver_id_model),
      control = lme4::lmerControl(calc.derivs = FALSE)
    )
  )
print(Sys.time() - .time)

serve_data_adj <- serve_data |>
  dplyr::mutate(
    sos_no_error_server = predict(serve_model,
      newdata = serve_data,
      re.form = ~ (1 | conf_id_defense) + (1 | team_id_defense) + (1 | receiver_id_model),
      allow.new.levels = TRUE
    ),
    sos_no_error_receiver = predict(serve_model,
      newdata = serve_data,
      re.form = ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | server_id_model),
      allow.new.levels = TRUE
    )
  )

coef_conf_offense <- coef(serve_model)$conf_id_offense |>
  tibble::rownames_to_column() |>
  dplyr::transmute(
    conf_id = as.integer(rowname),
    coef_conf_offense = `(Intercept)`
  )

coef_conf_defense <- coef(serve_model)$conf_id_defense |>
  tibble::rownames_to_column() |>
  dplyr::transmute(
    conf_id = as.integer(rowname),
    coef_conf_defense = `(Intercept)`
  )

coef_team_offense <- coef(serve_model)$team_id_offense |>
  tibble::rownames_to_column() |>
  dplyr::transmute(
    team_id = as.integer(rowname),
    coef_team_offense = `(Intercept)`
  )

coef_team_defense <- coef(serve_model)$team_id_defense |>
  tibble::rownames_to_column() |>
  dplyr::transmute(
    team_id = as.integer(rowname),
    coef_team_defense = `(Intercept)`
  )

coef_server <- coef(serve_model)$server_id_model |>
  tibble::rownames_to_column() |>
  dplyr::transmute(
    player_id = as.integer(rowname),
    coef_server = `(Intercept)`
  )

coef_receiver <- coef(serve_model)$receiver_id_model |>
  tibble::rownames_to_column() |>
  dplyr::transmute(
    player_id = as.integer(rowname),
    coef_receiver = `(Intercept)`
  )

player_serve <- player |>
  dplyr::mutate(player_id = as.integer(player_id)) |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(coef_conf_offense, by = c("conference_id" = "conf_id")) |>
  dplyr::left_join(coef_team_offense, by = "team_id") |>
  dplyr::left_join(coef_server, by = "player_id") |>
  dplyr::transmute(player_id, player_name, team_name,
    sum_coef = -coef_conf_offense - coef_team_offense - coef_server,
    coef_conf_offense = -coef_conf_offense
  ) |>
  dplyr::filter(!is.na(sum_coef)) |>
  tibble::as_tibble()

player_reception <- player |>
  dplyr::mutate(player_id = as.integer(player_id)) |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(coef_conf_defense, by = c("conference_id" = "conf_id")) |>
  dplyr::left_join(coef_team_defense, by = "team_id") |>
  dplyr::left_join(coef_receiver, by = "player_id") |>
  dplyr::transmute(player_id, player_name, team_name,
    sum_coef = coef_conf_defense + coef_team_defense + coef_receiver,
    coef_conf_defense
  ) |>
  dplyr::filter(!is.na(sum_coef)) |>
  tibble::as_tibble()

team_serve <- team |>
  dplyr::left_join(coef_conf_offense, by = c("conference_id" = "conf_id")) |>
  dplyr::left_join(coef_conf_defense, by = c("conference_id" = "conf_id")) |>
  dplyr::left_join(coef_team_offense, by = "team_id") |>
  dplyr::left_join(coef_team_defense, by = "team_id") |>
  dplyr::transmute(team_id, team_name, sum_coef = -coef_conf_offense + coef_conf_defense - coef_team_offense + coef_team_defense) |>
  dplyr::filter(!is.na(sum_coef)) |>
  tibble::as_tibble()

pg_serve <- serve_data_adj |>
  dplyr::select(-player_name, -team_id) |>
  dplyr::left_join(player, by = "player_id") |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(conference, by = "conference_id") |>
  dplyr::group_by(player_id, player_name, team_name, conference_name) |>
  dplyr::summarize(

    pg_raw = -mean(pg_is_error + pg_no_error),
    pg_adj = -mean(pg_is_error + pg_no_error - sos_no_error_server),

    n = dplyr::n(),
    pct_is_error = mean(is_error),
    pg_is_error = -mean(pg_is_error),

    n_no_error = sum(no_error),
    pct_ace = weighted.mean(evaluation_code == "#", w = no_error),
    pg_no_error = -mean(pg_no_error),
    pg_no_error_adj = -mean(pg_no_error - sos_no_error_server),

    .groups = "drop"
  ) |>
  dplyr::arrange(-pg_adj)

pg_reception <- serve_data_adj |>
  dplyr::select(-player_id, -player_name, -team_id) |>
  dplyr::rename(player_id = receiver_id) |>
  dplyr::left_join(player, by = "player_id") |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(conference, by = "conference_id") |>
  dplyr::group_by(player_id, player_name, team_name, conference_name) |>
  dplyr::summarize(
    n_reception = sum(no_error),
    pct_ace = weighted.mean(evaluation_code == "#", w = no_error),
    pg_reception_raw = weighted.mean(pg_no_error, w = no_error),
    pg_reception_adj = weighted.mean(pg_no_error - sos_no_error_receiver, w = no_error),
    .groups = "drop"
  )

attack <- contact_sideout_prob |>
  dplyr::left_join(libero, by = c("match_id", "set_number", "home_team_id" = "team_id")) |>
  dplyr::rename(home_libero_id = libero_id) |>
  dplyr::left_join(libero, by = c("match_id", "set_number", "visiting_team_id" = "team_id")) |>
  dplyr::rename(visiting_libero_id = libero_id) |>
  dplyr::mutate(
    is_error = evaluation_code == "=",
    is_no_error = !is_error,
    is_block = !is_error & dplyr::coalesce(dplyr::lead(skill, 1) == "Block", FALSE),
    is_no_block = !is_error & !is_block,
    is_block_error = is_block & dplyr::lead(evaluation_code, 1) == "=",
    is_block_no_error = is_block & !is_block_error,
    is_block_return = is_block_no_error & (
      dplyr::lead(evaluation_code, 1) == "#" |
      dplyr::coalesce(dplyr::lead(skill, 2) == "Dig", FALSE) & team_id == dplyr::lead(team_id, 2)
    ),
    is_block_through = is_block_no_error & (
      dplyr::coalesce(dplyr::lead(skill, 2) == "Dig", FALSE) & team_id != dplyr::lead(team_id, 2)
    ),
    outcome_eval = dplyr::case_when(
      # attack error
      is_error ~ "=",
      # clean attack (no block)
      !is_error & !is_block & dplyr::lead(skill, 1) %in% c("Dig", NA) ~ dplyr::coalesce(dplyr::lead(evaluation_code, 1), "="),
      # block error
      is_block_error ~ "=",
      # block touch and through
      is_block & !is_block_error & !is_block_return & dplyr::lead(skill, 2) %in% c("Dig", NA) ~ dplyr::coalesce(dplyr::lead(evaluation_code, 2), "="),
      # block touch and return
      is_block_return & dplyr::lead(skill, 2) %in% c("Dig", NA) ~ dplyr::coalesce(dplyr::lead(evaluation_code, 2), "=")
    ),
    # The outcome evaluation code is expressed from which team's perspective?
    outcome_team = ifelse(is_error | is_block_return, "offense", "defense"),

    defense_setter_position = ifelse(team_id == home_team_id, visiting_setter_position, home_setter_position),
    defense_player_id1 = ifelse(team_id == home_team_id, visiting_player_id1, home_player_id1),
    defense_player_id2 = ifelse(team_id == home_team_id, visiting_player_id2, home_player_id2),
    defense_player_id3 = ifelse(team_id == home_team_id, visiting_player_id3, home_player_id3),
    defense_player_id4 = ifelse(team_id == home_team_id, visiting_player_id4, home_player_id4),
    defense_player_id5 = ifelse(team_id == home_team_id, visiting_player_id5, home_player_id5),
    defense_player_id6 = ifelse(team_id == home_team_id, visiting_player_id6, home_player_id6),
    defense_libero_id = ifelse(team_id == home_team_id, visiting_libero_id, home_libero_id),

    offense_setter_position = ifelse(team_id == visiting_team_id, visiting_setter_position, home_setter_position),
    offense_player_id1 = ifelse(team_id == visiting_team_id, visiting_player_id1, home_player_id1),
    offense_player_id2 = ifelse(team_id == visiting_team_id, visiting_player_id2, home_player_id2),
    offense_player_id3 = ifelse(team_id == visiting_team_id, visiting_player_id3, home_player_id3),
    offense_player_id4 = ifelse(team_id == visiting_team_id, visiting_player_id4, home_player_id4),
    offense_player_id5 = ifelse(team_id == visiting_team_id, visiting_player_id5, home_player_id5),
    offense_player_id6 = ifelse(team_id == visiting_team_id, visiting_player_id6, home_player_id6),
    offense_libero_id = ifelse(team_id == visiting_team_id, visiting_libero_id, home_libero_id),

    defense_back_right_player_id = dplyr::case_when(
      defense_setter_position == 1 ~ defense_player_id1,  # setter
      defense_setter_position == 2 ~ defense_player_id5,  # right-side
      defense_setter_position == 3 ~ defense_player_id6,  # right-side
      defense_setter_position == 4 ~ defense_player_id1,  # right-side
      defense_setter_position == 5 ~ defense_player_id5,  # setter
      defense_setter_position == 6 ~ defense_player_id6   # setter
    ),
    defense_front_right_player_id = dplyr::case_when(
      defense_setter_position == 1 ~ defense_player_id4,  # right-side
      defense_setter_position == 2 ~ defense_player_id2,  # setter
      defense_setter_position == 3 ~ defense_player_id3,  # setter
      defense_setter_position == 4 ~ defense_player_id4,  # setter
      defense_setter_position == 5 ~ defense_player_id2,  # right-side
      defense_setter_position == 6 ~ defense_player_id3   # right-side
    ),
    defense_front_middle_player_id = dplyr::case_when(
      defense_setter_position == 1 ~ defense_player_id3,  # middle 1
      defense_setter_position == 2 ~ defense_player_id4,  # middle 1
      defense_setter_position == 3 ~ defense_player_id2,  # middle 2
      defense_setter_position == 4 ~ defense_player_id3,  # middle 2
      defense_setter_position == 5 ~ defense_player_id4,  # middle 2
      defense_setter_position == 6 ~ defense_player_id2   # middle 1
    ),
    defense_front_left_player_id = dplyr::case_when(
      defense_setter_position == 1 ~ defense_player_id2,  # OH2
      defense_setter_position == 2 ~ defense_player_id3,  # OH2
      defense_setter_position == 3 ~ defense_player_id4,  # OH2
      defense_setter_position == 4 ~ defense_player_id2,  # OH1
      defense_setter_position == 5 ~ defense_player_id3,  # OH1
      defense_setter_position == 6 ~ defense_player_id4   # OH1
    ),
    defense_back_left_player_id = defense_libero_id,
    defense_back_middle_player_id = dplyr::case_when(
      defense_setter_position == 1 ~ defense_player_id5,  # OH1
      defense_setter_position == 2 ~ defense_player_id6,  # OH1
      defense_setter_position == 3 ~ defense_player_id1,  # OH1
      defense_setter_position == 4 ~ defense_player_id5,  # OH2
      defense_setter_position == 5 ~ defense_player_id6,  # OH2
      defense_setter_position == 6 ~ defense_player_id1   # OH2
    ),



    offense_back_right_player_id = dplyr::case_when(
      offense_setter_position == 1 ~ offense_player_id1,  # setter
      offense_setter_position == 2 ~ offense_player_id5,  # right-side
      offense_setter_position == 3 ~ offense_player_id6,  # right-side
      offense_setter_position == 4 ~ offense_player_id1,  # right-side
      offense_setter_position == 5 ~ offense_player_id5,  # setter
      offense_setter_position == 6 ~ offense_player_id6   # setter
    ),
    offense_front_right_player_id = dplyr::case_when(
      offense_setter_position == 1 ~ offense_player_id4,  # right-side
      offense_setter_position == 2 ~ offense_player_id2,  # setter
      offense_setter_position == 3 ~ offense_player_id3,  # setter
      offense_setter_position == 4 ~ offense_player_id4,  # setter
      offense_setter_position == 5 ~ offense_player_id2,  # right-side
      offense_setter_position == 6 ~ offense_player_id3   # right-side
    ),
    offense_front_middle_player_id = dplyr::case_when(
      offense_setter_position == 1 ~ offense_player_id3,  # middle 1
      offense_setter_position == 2 ~ offense_player_id4,  # middle 1
      offense_setter_position == 3 ~ offense_player_id2,  # middle 2
      offense_setter_position == 4 ~ offense_player_id3,  # middle 2
      offense_setter_position == 5 ~ offense_player_id4,  # middle 2
      offense_setter_position == 6 ~ offense_player_id2   # middle 1
    ),
    offense_front_left_player_id = dplyr::case_when(
      offense_setter_position == 1 ~ offense_player_id2,  # OH2
      offense_setter_position == 2 ~ offense_player_id3,  # OH2
      offense_setter_position == 3 ~ offense_player_id4,  # OH2
      offense_setter_position == 4 ~ offense_player_id2,  # OH1
      offense_setter_position == 5 ~ offense_player_id3,  # OH1
      offense_setter_position == 6 ~ offense_player_id4   # OH1
    ),
    offense_back_left_player_id = offense_libero_id,
    offense_back_middle_player_id = dplyr::case_when(
      offense_setter_position == 1 ~ offense_player_id5,  # OH1
      offense_setter_position == 2 ~ offense_player_id6,  # OH1
      offense_setter_position == 3 ~ offense_player_id1,  # OH1
      offense_setter_position == 4 ~ offense_player_id5,  # OH2
      offense_setter_position == 5 ~ offense_player_id6,  # OH2
      offense_setter_position == 6 ~ offense_player_id1   # OH2
    ),

    player_rotation = dplyr::case_when(
      player_id == offense_player_id1 ~ 1,
      player_id == offense_player_id2 ~ 2,
      player_id == offense_player_id3 ~ 3,
      player_id == offense_player_id4 ~ 4,
      player_id == offense_player_id5 ~ 5,
      player_id == offense_player_id6 ~ 6
    ),

    player_position = dplyr::case_when(
      player_id == offense_front_left_player_id ~ "OH",
      player_id == offense_front_middle_player_id ~ "MB",
      player_id == offense_front_right_player_id ~ "RS",
      player_id %in% c(offense_back_left_player_id, offense_back_middle_player_id, offense_back_right_player_id) ~ "BR"
    ),

    player_zone = dplyr::case_when(
      player_id == offense_back_left_player_id ~ "BL",
      player_id == offense_back_middle_player_id ~ "BM",
      player_id == offense_back_right_player_id ~ "BR",
      player_id == offense_front_left_player_id ~ "FL",
      player_id == offense_front_middle_player_id ~ "FM",
      player_id == offense_front_right_player_id ~ "FR",
      TRUE ~ "BL" # libero
    ),

    setter_id = ifelse(dplyr::lag(skill, 1) == "Set", dplyr::lag(player_id, 1), NA),
    blocker_id = ifelse(dplyr::lead(skill, 1) == "Block", dplyr::lead(player_id, 1), NA),
    digger_id = dplyr::case_when(
      dplyr::lead(skill, 1) == "Dig" ~ dplyr::lead(player_id, 1),
      dplyr::lead(skill, 1) == "Block" & dplyr::lead(skill, 2) == "Dig" ~ dplyr::lead(player_id, 2),
      TRUE ~ NA
    )
  )

volley_start <- attack |>
  dplyr::group_by(match_id, point_id, team_touch_id) |>
  dplyr::filter(skill != "Block") |>
  dplyr::slice(1) |>
  dplyr::transmute(match_id, point_id, team_touch_id,
    volley_start = paste0(substring(skill, 1, 1), evaluation_code)
  )

blocker_responsibility <- attack |>
  dplyr::mutate(
    blocker_resp = ifelse(dplyr::lead(skill, 1) == "Block", dplyr::lead(player_zone, 1), NA)
  ) |>
  dplyr::count(attack_code, blocker_resp) |>
  dplyr::filter(!is.na(blocker_resp)) |>
  dplyr::group_by(attack_code) |>
  dplyr::arrange(-n) |>
  dplyr::slice(1) |>
  dplyr::ungroup() |>
  dplyr::select(attack_code, blocker_resp)

digger_responsibility <- attack |>
  dplyr::mutate(
    digger_resp = ifelse(dplyr::lead(skill, 1) == "Dig", dplyr::lead(player_zone, 1), NA)
  ) |>
  dplyr::count(attack_code, end_zone, digger_resp) |>
  dplyr::filter(!is.na(digger_resp), !is.na(attack_code)) |>
  dplyr::group_by(attack_code, end_zone) |>
  dplyr::arrange(-n) |>
  dplyr::slice(1) |>
  dplyr::ungroup() |>
  dplyr::select(attack_code, end_zone, digger_resp)

attack_data <- attack |>
  dplyr::mutate(
    skill_lead_1 = dplyr::lead(skill, 1),
    skill_lead_2 = dplyr::lead(skill, 2)
  ) |>
  dplyr::filter(
    skill_lead_1 %in% c("Block", "Dig", NA),
    skill_lead_1 %in% c("Dig", NA) | skill_lead_2 %in% c("Dig", NA),
    skill == "Attack",
    !is_block_no_error | xor(is_block_return, is_block_through)
  ) |>
  dplyr::left_join(volley_start, by = c("match_id", "point_id", "team_touch_id")) |>
  dplyr::left_join(blocker_responsibility, by = "attack_code") |>
  dplyr::left_join(digger_responsibility, by = c("attack_code", "end_zone")) |>
  dplyr::left_join(win_prob_from_dig, by = c("outcome_eval" = "eval")) |>
  dplyr::filter(substring(volley_start, 1, 1) %in% c("D", "R", "F")) |>
  dplyr::mutate(
    blocker_id = dplyr::case_when(
      !is.na(blocker_id) ~ blocker_id,
      blocker_resp == "FL" ~ defense_front_left_player_id,
      blocker_resp == "FM" ~ defense_front_middle_player_id,
      blocker_resp == "FR" ~ defense_front_right_player_id
    ),
    digger_id = dplyr::case_when(
      !is.na(digger_id) ~ digger_id,
      digger_resp == "FL" ~ defense_front_left_player_id,
      digger_resp == "FM" ~ defense_front_middle_player_id,
      digger_resp == "FR" ~ defense_front_right_player_id,
      digger_resp == "BL" ~ defense_back_left_player_id,
      digger_resp == "BM" ~ defense_back_middle_player_id,
      digger_resp == "BR" ~ defense_back_right_player_id
    ),
    win_prob = ifelse(outcome_team == "offense", win_prob, 1 - win_prob)
  )

exp_win_prob <- attack_data |>
  dplyr::filter(!is.na(win_prob), !is.na(attack_code)) |>
  dplyr::group_by(volley_start, attack_code) |>
  dplyr::summarize(
    n = dplyr::n(),
    exp = mean(win_prob),
    exp_error = 0,
    exp_no_error = weighted.mean(win_prob, w = is_no_error),
    exp_block = weighted.mean(win_prob, w = is_block),
    exp_no_block = weighted.mean(win_prob, w = is_no_block),
    exp_block_error = 1,
    exp_block_no_error = weighted.mean(win_prob, w = is_block_no_error),
    exp_block_return = weighted.mean(win_prob, w = is_block_return),
    exp_block_through = weighted.mean(win_prob, w = is_block_through),
    .groups = "drop"
  ) |>
  dplyr::filter(n >= 100) |>
  dplyr::arrange(-exp)

attack_data_with_pg <- attack_data |>
  dplyr::filter(
    player_id != "unknown player",
    !is.na(conf_id_home), !is.na(conf_id_away), !is.na(team_id_home), !is.na(team_id_home), !is.na(win_prob)
  ) |>
  dplyr::inner_join(exp_win_prob, by = c("volley_start", "attack_code")) |>
  dplyr::mutate(
    pg_is_error = dplyr::case_when(is_error ~ exp_error, is_no_error ~ exp_no_error) - exp,
    pg_is_block = dplyr::case_when(is_block ~ exp_block, is_no_block ~ exp_no_block, TRUE ~ exp_no_error) - exp_no_error,
    pg_is_block_error = dplyr::case_when(is_block_error ~ exp_block_error, is_block_no_error ~ exp_block_no_error, TRUE ~ exp_block) - exp_block,
    pg_is_block_return = dplyr::case_when(is_block_return ~ exp_block_return, is_block_through ~ exp_block_through, TRUE ~ exp_block_no_error) - exp_block_no_error,
    pg_no_block = ifelse(is_no_block, win_prob - exp_no_block, 0),
    pg_block_return = ifelse(is_block_return, win_prob - exp_block_return, 0),
    pg_block_through = ifelse(is_block_through, win_prob - exp_block_through, 0)
  ) |>
  dplyr::group_by(player_id) |>
  dplyr::mutate(count = length(player_id)) |>
  dplyr::ungroup() |>
  dplyr::mutate(player_id_model = ifelse(count < 200, "0", player_id)) |>
  dplyr::group_by(setter_id) |>
  dplyr::mutate(count = length(setter_id)) |>
  dplyr::ungroup() |>
  dplyr::mutate(setter_id_model = ifelse(count < 1000, "0", setter_id)) |>
  dplyr::group_by(blocker_id) |>
  dplyr::mutate(count = length(blocker_id)) |>
  dplyr::ungroup() |>
  dplyr::mutate(blocker_id_model = ifelse(count < 200, "0", blocker_id)) |>
  dplyr::group_by(digger_id) |>
  dplyr::mutate(count = length(digger_id)) |>
  dplyr::ungroup() |>
  dplyr::mutate(digger_id_model = ifelse(count < 100, "0", digger_id))

attack_model <- list()

.time <- Sys.time()
attack_model$is_error <- lme4::lmer(
  formula = pg_is_error ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | setter_id_model),
  data = attack_data_with_pg,
  control = lme4::lmerControl(calc.derivs = FALSE)
)
sd_attacker <- attr(lme4::VarCorr(attack_model$is_error)$player_id_model, 'stddev')
sd_setter <- attr(lme4::VarCorr(attack_model$is_error)$setter_id_model, 'stddev')
pct_is_error_attacker <- sd_attacker / (sd_attacker + sd_setter)
pct_is_error_setter <- sd_setter / (sd_attacker + sd_setter)
print(Sys.time() - .time)

.time <- Sys.time()
attack_model$is_block <- lme4::lmer(
  formula = pg_is_block ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | conf_id_defense) + (1 | team_id_defense) + (1 | setter_id_model) + (1 | blocker_id_model),
  data = attack_data_with_pg |>
    dplyr::filter(is_no_error),
  control = lme4::lmerControl(calc.derivs = FALSE)
)
sd_attacker <- attr(lme4::VarCorr(attack_model$is_block)$player_id_model, 'stddev')
sd_setter <- attr(lme4::VarCorr(attack_model$is_block)$setter_id_model, 'stddev')
pct_is_block_attacker <- sd_attacker / (sd_attacker + sd_setter)
pct_is_block_setter <- sd_setter / (sd_attacker + sd_setter)
print(Sys.time() - .time) # 16 minutes

.time <- Sys.time()
attack_model$is_block_error <- lme4::lmer(
  formula = pg_is_block_error ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | conf_id_defense) + (1 | team_id_defense) + (1 | setter_id_model) + (1 | blocker_id_model),
  data = attack_data_with_pg |>
    dplyr::filter(is_block),
  control = lme4::lmerControl(calc.derivs = FALSE)
)
sd_attacker <- attr(lme4::VarCorr(attack_model$is_block_error)$player_id_model, 'stddev')
sd_setter <- attr(lme4::VarCorr(attack_model$is_block_error)$setter_id_model, 'stddev')
pct_is_block_error_attacker <- sd_attacker / (sd_attacker + sd_setter)
pct_is_block_error_setter <- sd_setter / (sd_attacker + sd_setter)
print(Sys.time() - .time) # 13 minutes

.time <- Sys.time()
attack_model$is_block_return <- lme4::lmer(
  formula = pg_is_block_return ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | conf_id_defense) + (1 | team_id_defense) + (1 | setter_id_model) + (1 | blocker_id_model),
  data = attack_data_with_pg |>
    dplyr::filter(is_block_no_error),
  control = lme4::lmerControl(calc.derivs = FALSE)
)
sd_attacker <- attr(lme4::VarCorr(attack_model$is_block_return)$player_id_model, 'stddev')
sd_setter <- attr(lme4::VarCorr(attack_model$is_block_return)$setter_id_model, 'stddev')
pct_is_block_return_attacker <- sd_attacker / (sd_attacker + sd_setter)
pct_is_block_return_setter <- sd_setter / (sd_attacker + sd_setter)
print(Sys.time() - .time) # 11 minutes

.time <- Sys.time()
attack_model$no_block <- lme4::lmer(
  formula = pg_no_block ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | conf_id_defense) + (1 | team_id_defense) + (1 | setter_id_model) + (1 | blocker_id_model) + (1 | digger_id_model),
  data = attack_data_with_pg |>
    dplyr::filter(is_no_block),
  control = lme4::lmerControl(calc.derivs = FALSE, optimizer ="Nelder_Mead")
)
sd_attacker <- attr(lme4::VarCorr(attack_model$no_block)$player_id_model, 'stddev')
sd_setter <- attr(lme4::VarCorr(attack_model$no_block)$setter_id_model, 'stddev')
pct_no_block_attacker <- sd_attacker / (sd_attacker + sd_setter)
pct_no_block_setter <- sd_setter / (sd_attacker + sd_setter)
sd_blocker <- attr(lme4::VarCorr(attack_model$no_block)$player_id_model, 'stddev')
sd_digger <- attr(lme4::VarCorr(attack_model$no_block)$digger_id_model, 'stddev')
pct_no_block_blocker <- sd_blocker / (sd_blocker + sd_digger)
pct_no_block_digger <- sd_digger / (sd_blocker + sd_digger)
print(Sys.time() - .time) # 1.2 hours

.time <- Sys.time()
attack_model$block_through <- lme4::lmer(
  formula = pg_block_through ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | conf_id_defense) + (1 | team_id_defense) + (1 | setter_id_model) + (1 | blocker_id_model) + (1 | digger_id_model),
  data = attack_data_with_pg |>
    dplyr::filter(is_block_through),
  control = lme4::lmerControl(calc.derivs = FALSE)
)
sd_attacker <- attr(lme4::VarCorr(attack_model$block_through)$player_id_model, 'stddev')
sd_setter <- attr(lme4::VarCorr(attack_model$block_through)$setter_id_model, 'stddev')
pct_block_through_attacker <- sd_attacker / (sd_attacker + sd_setter)
pct_block_through_setter <- sd_setter / (sd_attacker + sd_setter)
sd_blocker <- attr(lme4::VarCorr(attack_model$block_through)$player_id_model, 'stddev')
sd_digger <- attr(lme4::VarCorr(attack_model$block_through)$digger_id_model, 'stddev')
pct_block_through_blocker <- sd_blocker / (sd_blocker + sd_digger)
pct_block_through_digger <- sd_digger / (sd_blocker + sd_digger)
print(Sys.time() - .time) # 11 minutes

.time <- Sys.time()
attack_model$block_return <- lme4::lmer(
  formula = pg_block_return ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | conf_id_defense) + (1 | team_id_defense) + (1 | setter_id_model) + (1 | blocker_id_model),
  data = attack_data_with_pg |>
    dplyr::filter(is_block_return),
  control = lme4::lmerControl(calc.derivs = FALSE)
)
sd_attacker <- attr(lme4::VarCorr(attack_model$block_return)$player_id_model, 'stddev')
sd_setter <- attr(lme4::VarCorr(attack_model$block_return)$setter_id_model, 'stddev')
pct_block_return_attacker <- sd_attacker / (sd_attacker + sd_setter)
pct_block_return_setter <- sd_setter / (sd_attacker + sd_setter)
print(Sys.time() - .time) # 12 minutes

attack_data_with_pg_adj <- attack_data_with_pg |>
  dplyr::mutate(
    sos_is_block_offense = predict(attack_model$is_block, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_defense) + (1 | team_id_defense) + (1 | blocker_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_is_block_defense = predict(attack_model$is_block, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | setter_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_is_block_error_offense = predict(attack_model$is_block_error, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_defense) + (1 | team_id_defense) + (1 | blocker_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_is_block_error_defense = predict(attack_model$is_block_error, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | setter_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_is_block_return_offense = predict(attack_model$is_block_return, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_defense) + (1 | team_id_defense) + (1 | blocker_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_is_block_return_defense = predict(attack_model$is_block_return, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | setter_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_no_block_offense = predict(attack_model$no_block, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_defense) + (1 | team_id_defense) + (1 | blocker_id_model) + (1 | digger_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_no_block_defense = predict(attack_model$no_block, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | setter_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_block_through_offense = predict(attack_model$block_through, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_defense) + (1 | team_id_defense) + (1 | blocker_id_model) + (1 | digger_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_block_through_defense = predict(attack_model$block_through, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | setter_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_block_return_offense = predict(attack_model$block_return, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_defense) + (1 | team_id_defense) + (1 | blocker_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_block_return_defense = predict(attack_model$block_return, newdata = attack_data_with_pg, re.form = ~ (1 | conf_id_offense) + (1 | team_id_offense) + (1 | player_id_model) + (1 | setter_id_model), allow.new.levels = TRUE) |>
      scale(scale = FALSE),
    sos_offense = sos_is_block_offense * is_no_error +
      sos_is_block_error_offense * is_block +
      sos_is_block_return_offense * is_block +
      sos_no_block_offense * is_no_block +
      sos_block_through_offense * is_block_through +
      sos_block_return_defense * is_block_return
  )


pg_attack <- attack_data_with_pg_adj |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(conference, by = "conference_id") |>
  dplyr::group_by(player_id, player_name, team_name, conference_name, player_position) |>
  dplyr::summarize(

    n = dplyr::n(),
    pct_is_error = mean(is_error),
    pg_is_error_raw = mean(pg_is_error) * pct_is_error_attacker,
    pg_is_error_adj = mean(pg_is_error) * pct_is_error_attacker,  # no adjustment

    n_no_error = sum(is_no_error),
    pct_is_block = weighted.mean(is_block, w = is_no_error),
    pg_is_block_raw = mean(pg_is_block) * pct_is_block_attacker,
    pg_is_block_adj = mean(pg_is_block - sos_is_block_offense * is_no_error) * pct_is_block_attacker,

    n_block = sum(is_block),
    pct_is_block_error = weighted.mean(is_block_error, w = is_block),
    pg_is_block_error_raw = mean(pg_is_block_error) * pct_is_block_error_attacker,
    pg_is_block_error_adj = mean(pg_is_block_error - sos_is_block_error_offense * is_block) * pct_is_block_error_attacker,

    n_block_no_error = sum(is_block_no_error),
    pct_is_block_through = weighted.mean(is_block_through, w = is_block_no_error),
    pg_is_block_return_raw = mean(pg_is_block_return) * pct_is_block_return_attacker,
    pg_is_block_return_adj = mean(pg_is_block_return - sos_is_block_return_offense * is_block_no_error) * pct_is_block_return_attacker,

    n_no_block = sum(is_no_block),
    pct_kill_no_block = weighted.mean(win_prob > 0.99, w = is_no_block),
    pg_no_block_raw = mean(pg_no_block) * pct_no_block_attacker,
    pg_no_block_adj = mean(pg_no_block - sos_no_block_offense * is_no_block) * pct_no_block_attacker,

    n_block_through = sum(is_block_through),
    pct_kill_block_through = weighted.mean(win_prob > 0.99, w = is_block_through),
    pg_block_through_raw = mean(pg_block_through) * pct_block_through_attacker,
    pg_block_through_adj = mean(pg_block_through - sos_block_through_offense * is_block_through) * pct_block_through_attacker,

    n_block_return = sum(is_block_return),
    pct_stuff = weighted.mean(win_prob < 0.01, w = is_block_return),
    pg_block_return_raw = mean(pg_block_return) * pct_block_return_attacker,
    pg_block_return_adj = mean(pg_block_return - sos_block_return_offense * is_block_return) * pct_block_return_attacker,

    .groups = "drop"
  ) |>
  dplyr::mutate(
    pg_raw = pg_is_error_raw + pg_is_block_raw + pg_is_block_error_raw + pg_is_block_return_raw + pg_no_block_raw + pg_block_return_raw + pg_block_through_raw,
    pg_adj = pg_is_error_adj + pg_is_block_adj + pg_is_block_error_adj + pg_is_block_return_adj + pg_no_block_adj + pg_block_return_adj + pg_block_through_adj,
  ) |>
  dplyr::select(player_id, player_name, team_name, conference_name, player_position, pg_raw, pg_adj, dplyr::everything()) |>
  dplyr::arrange(-pg_adj)

pg_set <- attack_data_with_pg_adj |>
  dplyr::select(-player_id, -player_name) |>
  dplyr::rename(player_id = setter_id) |>
  dplyr::left_join(dplyr::select(player, player_id, player_name), by = "player_id") |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(conference, by = "conference_id") |>
  dplyr::group_by(player_id, player_name, team_name, conference_name) |>
  dplyr::summarize(

    n = dplyr::n(),
    pct_is_error = mean(is_error),
    pg_is_error_raw = mean(pg_is_error) * pct_is_error_setter,
    pg_is_error_adj = mean(pg_is_error) * pct_is_error_setter,  # no adjustment

    n_no_error = sum(is_no_error),
    pct_is_block = weighted.mean(is_block, w = is_no_error),
    pg_is_block_raw = mean(pg_is_block) * pct_is_block_setter,
    pg_is_block_adj = mean(pg_is_block - sos_is_block_offense * is_no_error) * pct_is_block_setter,

    n_block = sum(is_block),
    pct_is_block_error = weighted.mean(is_block_error, w = is_block),
    pg_is_block_error_raw = mean(pg_is_block_error) * pct_is_block_error_setter,
    pg_is_block_error_adj = mean(pg_is_block_error - sos_is_block_error_offense * is_block) * pct_is_block_error_setter,

    n_block_no_error = sum(is_block_no_error),
    pct_is_block_through = weighted.mean(is_block_through, w = is_block_no_error),
    pg_is_block_return_raw = mean(pg_is_block_return) * pct_is_block_return_setter,
    pg_is_block_return_adj = mean(pg_is_block_return - sos_is_block_return_offense * is_block_no_error) * pct_is_block_return_setter,

    n_no_block = sum(is_no_block),
    pct_kill_no_block = weighted.mean(win_prob > 0.99, w = is_no_block),
    pg_no_block_raw = mean(pg_no_block) * pct_no_block_setter,
    pg_no_block_adj = mean(pg_no_block - sos_no_block_offense * is_no_block) * pct_no_block_setter,

    n_block_through = sum(is_block_through),
    pct_kill_block_through = weighted.mean(win_prob > 0.99, w = is_block_through),
    pg_block_through_raw = mean(pg_block_through) * pct_block_through_setter,
    pg_block_through_adj = mean(pg_block_through - sos_block_through_offense * is_block_through) * pct_block_through_setter,

    n_block_return = sum(is_block_return),
    pct_stuff = weighted.mean(win_prob < 0.01, w = is_block_return),
    pg_block_return_raw = mean(pg_block_return) * pct_block_return_setter,
    pg_block_return_adj = mean(pg_block_return - sos_block_return_offense * is_block_return) * pct_block_return_setter,

    .groups = "drop"
  ) |>
  dplyr::mutate(
    pg_raw = pg_is_error_raw + pg_is_block_raw + pg_is_block_error_raw + pg_is_block_return_raw + pg_no_block_raw + pg_block_return_raw + pg_block_through_raw,
    pg_adj = pg_is_error_adj + pg_is_block_adj + pg_is_block_error_adj + pg_is_block_return_adj + pg_no_block_adj + pg_block_return_adj + pg_block_through_adj
  ) |>
  dplyr::select(player_id, player_name, team_name, conference_name, pg_raw, pg_adj, dplyr::everything()) |>
  dplyr::arrange(-pg_adj)

pg_block <- attack_data_with_pg_adj |>
  dplyr::select(-player_id, -player_name, -team_id) |>
  dplyr::rename(player_id = blocker_id) |>
  dplyr::left_join(player, by = "player_id") |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(conference, by = "conference_id") |>
  dplyr::group_by(player_id, player_name, team_name, conference_name) |>
  dplyr::summarize(

    n = sum(is_no_error),
    pct_is_block = weighted.mean(is_block, w = is_no_error),
    pg_is_block_raw = -weighted.mean(pg_is_block, w = is_no_error),
    pg_is_block_adj = -weighted.mean(pg_is_block - sos_is_block_defense * is_no_error, w = is_no_error),

    n_block = sum(is_block),
    pct_is_block_error = weighted.mean(is_block_error, w = is_block),
    pg_is_block_error_raw = -weighted.mean(pg_is_block_error, w = is_no_error),
    pg_is_block_error_adj = -weighted.mean(pg_is_block_error - sos_is_block_error_defense * is_block, w = is_no_error),

    n_block_no_error = sum(is_block_no_error),
    pct_is_block_through = weighted.mean(is_block_through, w = is_block_no_error),
    pg_is_block_return_raw = -weighted.mean(pg_is_block_return, w = is_no_error),
    pg_is_block_return_adj = -weighted.mean(pg_is_block_return - sos_is_block_return_defense * is_block_no_error, w = is_no_error),

    n_no_block = sum(is_no_block),
    pct_kill_no_block = weighted.mean(win_prob > 0.99, w = is_no_block),
    pg_no_block_raw = -weighted.mean(pg_no_block, w = is_no_error) / 2,
    pg_no_block_adj = -weighted.mean(pg_no_block - sos_no_block_defense * is_no_block, w = is_no_error) / 2,

    n_block_through = sum(is_block_through),
    pct_kill_block_through = weighted.mean(win_prob > 0.99, w = is_block_through),
    pg_block_through_raw = -weighted.mean(pg_block_through, w = is_no_error) / 2,
    pg_block_through_adj = -weighted.mean(pg_block_through - sos_block_through_defense * is_block_through, w = is_no_error) / 2,

    n_block_return = sum(is_block_return),
    pct_stuff = weighted.mean(win_prob < 0.01, w = is_block_return),
    pg_block_return_raw = -weighted.mean(pg_block_return, w = is_no_error),
    pg_block_return_adj = -weighted.mean(pg_block_return - sos_block_return_defense * is_block_return, w = is_no_error),

    .groups = "drop"
  ) |>
  dplyr::mutate(
    pg_raw = pg_is_block_raw + pg_is_block_error_raw + pg_is_block_return_raw + pg_no_block_raw + pg_block_return_raw + pg_block_through_raw,
    pg_adj = pg_is_block_adj + pg_is_block_error_adj + pg_is_block_return_adj + pg_no_block_adj + pg_block_return_adj + pg_block_through_adj
  ) |>
  dplyr::select(player_id, player_name, team_name, conference_name, pg_raw, pg_adj, dplyr::everything()) |>
  dplyr::arrange(-pg_adj)

pg_dig <- attack_data_with_pg_adj |>
  dplyr::select(-player_id, -player_name, -team_id) |>
  dplyr::rename(player_id = digger_id) |>
  dplyr::left_join(player, by = "player_id") |>
  dplyr::left_join(team, by = "team_id") |>
  dplyr::left_join(conference, by = "conference_id") |>
  dplyr::group_by(player_id, player_name, team_name, conference_name) |>
  dplyr::summarize(

    n_dig = sum(is_no_block + is_block_through),
    pct_kill = weighted.mean(win_prob > 0.99, w = is_no_block + is_block_through),
    pg_dig_raw = -weighted.mean(
      x = pg_no_block * pct_no_block_digger + pg_block_through * pct_block_through_digger,
      w = is_no_block + is_block_through
    ),
    pg_dig_adj = -weighted.mean(
      x = (pg_no_block - sos_no_block_defense * is_no_block) * pct_no_block_digger + (pg_block_through - sos_block_through_defense * is_block_through) * pct_block_through_digger,
      w = is_no_block + is_block_through
    ),

    n_no_block = sum(is_no_block),
    pct_kill_no_block = weighted.mean(win_prob > 0.99, w = is_no_block),
    pg_no_block_raw = -weighted.mean(pg_no_block * pct_no_block_digger, w = is_no_block),
    pg_no_block_adj = -weighted.mean((pg_no_block - sos_no_block_defense * is_no_block) * pct_no_block_digger, w = is_no_block),

    n_block_through = sum(is_block_through),
    pct_kill_block_through = weighted.mean(win_prob > 0.99, w = is_block_through),
    pg_block_through_raw = -weighted.mean(pg_block_through * pct_block_through_digger, w = is_block_through),
    pg_block_through_raw = -weighted.mean((pg_block_through - sos_block_through_defense * is_block_through) * pct_block_through_digger, w = is_block_through),

    .groups = "drop"
  ) |>
  dplyr::arrange(-pg_dig_adj)

pg_pass <- pg_reception |>
  dplyr::left_join(pg_dig, by = c("player_id", "player_name", "team_name", "conference_name")) |>
  dplyr::mutate(
    n_reception = dplyr::coalesce(n_reception, 0),
    n_dig = dplyr::coalesce(n_dig, 0),
    n = n_reception + n_dig,
    pg_raw = (n_reception * pg_reception_raw + n_dig * pg_dig_raw) / n,
    pg_adj = (n_reception * pg_reception_adj + n_dig * pg_dig_adj) / n
  ) |>
  dplyr::select(player_id, player_name, team_name, conference_name, n, pg_raw, pg_adj, dplyr::everything()) |>
  dplyr::arrange(-pg_adj)

sets_played <- contact_sideout_prob |>
  dplyr::filter(!is.na(player_id)) |>
  dplyr::count(player_id, match_id, set_number) |>
  dplyr::count(player_id) |>
  dplyr::rename(sets_played = n)

id_columns <- c("player_id", "player_name", "team_name", "conference_name")

pg_overall <- sets_played |>
  dplyr::full_join(
    y = pg_serve |>
      dplyr::mutate(pg_raw = n * pg_raw, pg_adj = n * pg_adj) |>
      dplyr::select(dplyr::all_of(id_columns), n_serve = n, pg_serve_raw = pg_raw, pg_serve_adj = pg_adj),
    by = "player_id"
  ) |>
  dplyr::full_join(
    y = pg_pass |>
      dplyr::mutate(pg_raw = n * pg_raw, pg_adj = n * pg_adj) |>
      dplyr::select(dplyr::all_of(id_columns), n_pass = n, pg_pass_raw = pg_raw, pg_pass_adj = pg_adj),
    by = id_columns
  ) |>
  dplyr::full_join(
    y = pg_set |>
      dplyr::mutate(pg_raw = n * pg_raw, pg_adj = n * pg_adj) |>
      dplyr::select(dplyr::all_of(id_columns), n_set = n, pg_set_raw = pg_raw, pg_set_adj = pg_adj),
    by = id_columns
  ) |>
  dplyr::full_join(
    y = pg_attack |>
      dplyr::mutate(pg_raw = n * pg_raw, pg_adj = n * pg_adj) |>
      dplyr::group_by_at(id_columns) |>
      dplyr::summarize(n_attack = sum(n), pg_attack_raw = sum(pg_raw), pg_attack_adj = sum(pg_adj), .groups = "drop"),
    by = id_columns
  ) |>
  dplyr::full_join(
    y = pg_block |>
      dplyr::mutate(pg_raw = n * pg_raw, pg_adj = n * pg_adj) |>
      dplyr::select(dplyr::all_of(id_columns), n_block = n, pg_block_raw = pg_raw, pg_block_adj = pg_adj),
    by = id_columns
  ) |>
  dplyr::mutate_at(
    .vars = dplyr::vars(dplyr::matches("^sets_played|^n_|^pg_")),
    .funs = function(x) tidyr::replace_na(x, 0)
  )

pg_overall_per_set <- pg_overall |>
  dplyr::mutate_at(
    .vars = dplyr::vars(dplyr::matches("^n_|^pg_")),
    .funs = function(x, denominator) x / denominator,
    denominator = pg_overall$sets_played
  ) |>
  dplyr::filter(sets_played > 0) |>
  dplyr::mutate(
    n = n_serve + n_pass + n_set + n_attack + n_block,
    pg_raw = pg_serve_raw + pg_pass_raw + pg_set_raw + pg_attack_raw + pg_block_raw,
    pg_adj = pg_serve_adj + pg_pass_adj + pg_set_adj + pg_attack_adj + pg_block_adj,
  ) |>
  dplyr::select(dplyr::all_of(id_columns), sets_played, n, pg_raw, pg_adj, dplyr::everything()) |>
  dplyr::arrange(-pg_adj)


pg_overall_per_set |>
  dplyr::filter(sets_played >= 50) |>
  write.csv(file = "~/Downloads/pg_overall.csv", row.names = FALSE, na = "")
pg_serve |>
  dplyr::filter(n >= 100) |>
  write.csv(file = "~/Downloads/pg_serve.csv", row.names = FALSE, na = "")
pg_pass |>
  dplyr::filter(n >= 200) |>
  write.csv(file = "~/Downloads/pg_pass.csv", row.names = FALSE, na = "")
pg_set |>
  dplyr::filter(n >= 1000) |>
  write.csv(file = "~/Downloads/pg_set.csv", row.names = FALSE, na = "")
pg_attack |>
  dplyr::filter(n >= 200) |>
  write.csv(file = "~/Downloads/pg_attack.csv", row.names = FALSE, na = "")
pg_block |>
  dplyr::filter(n >= 200) |>
  write.csv(file = "~/Downloads/pg_block.csv", row.names = FALSE, na = "")




rice_blue <- rgb(0.000, 0.125, 0.357, maxColorValue = 1)
rice_blue_transparent <- rgb(0.000, 0.125, 0.357, alpha = 0.5, maxColorValue = 1)
rice_gray <- rgb(0.486, 0.494, 0.498, maxColorValue = 1)
rice_gray_transparent <- rgb(0.486, 0.494, 0.498, alpha = 0.5, maxColorValue = 1)
rice_rich_blue <- rgb(0.039, 0.314, 0.620, maxColorValue = 1)
rice_rich_blue_transparent <- rgb(0.039, 0.314, 0.620, alpha = 0.5, maxColorValue = 1)

{
  pdf("~/Downloads/points_gained_per_opportunity.pdf", height = 6, width = 9)
  par(mfrow = c(2, 3))
  pg_hist <- function(x, ...) {
    hist(
      x = x,
      breaks = c(-1, seq(from = -0.1, to = 0.1, by = 0.01), 1),
      xlim = c(-0.1, 0.1),
      xlab = "",
      ylab = "",
      axes = FALSE,
      col = rice_blue_transparent,
      border = rice_blue_transparent,
      ...
    )
    axis(
      side = 1,
      at = seq(from = -0.1, to = 0.1, by = 0.05),
      labels = c("-10%", "-5%", "0%", "5%", "10%")
    )
  }
  pg_serve |>
    dplyr::filter(n >= 100) |>
    with(pg_hist(pg_raw, main = "Serve"))
  pg_set |>
    dplyr::filter(n >= 1000) |>
    with(pg_hist(pg_raw, main = "Set"))
  pg_attack |>
    dplyr::filter(n >= 200) |>
    with(pg_hist(pg_raw, main = "Attack"))
  pg_pass |>
    dplyr::filter(n_reception >= 100) |>
    with(pg_hist(pg_reception_raw, main = "Reception"))
  pg_pass |>
    dplyr::filter(n_dig >= 100) |>
    with(pg_hist(pg_dig_raw, main = "Dig"))
  pg_block |>
    dplyr::filter(n >= 200) |>
    with(pg_hist(pg_raw, main = "Block"))
  dev.off()
}

pg_pass |>
  dplyr::filter(n_dig >= 200) |>
  dplyr::filter(1 - pct_kill > 0.715, 1 - pct_kill < 0.725) |>
  dplyr::arrange(-pg_dig_raw) |>
  dplyr::select(player_name, team_name, n, pg_dig_raw)

attack_data_with_pg |>
  dplyr::filter(is_no_block | is_block_through, digger_id %in% c("-310825", "-283353")) |>  # Taylor Quan and Temi Thomas
  dplyr::group_by(digger_id) |>
  dplyr::summarize(
    pct_no_block = mean(is_no_block),
    pct_perfect_dig = mean(outcome_team == "defense" & outcome_eval == "#")
  )

{
  pdf("~/Downloads/dig_comparison.pdf")
  pg_pass |>
    dplyr::filter(n_dig >= 200) |>
    with(
      plot(
        x = 1 - pct_kill,
        y = pg_dig_raw,
        xlab = "Digs per Opportunity (min. 200 opportunities)",
        ylab = "Points Gained per Opportunity",
        axes = FALSE,
        type = "n",
        xlim = c(0.5, 0.8),
        ylim = c(-0.05, 0.05)
      )
    )
    abline(h = 0, lty = 2, col = rice_gray_transparent)
    abline(v = 0.65, lty = 2, col = rice_gray_transparent)
    axis(1, at = c(0.5, 0.65, 0.8), labels = c("50%", "65%", "80%"))
    axis(2, at = c(-0.05, 0, 0.05), labels = c("-5%", "0%", "+5%"))
  pg_pass |>
    dplyr::filter(n_dig >= 200) |>
    with(points(x = 1 - pct_kill, y = pg_dig_raw, col = rice_gray_transparent, pch = 1, cex = 0.8))
  pg_pass |>
    dplyr::filter(player_id %in% c("-310825", "-283353")) |>  # Taylor Quan and Temi Thomas
    with(points(x = 1 - pct_kill, y = pg_dig_raw, col = rgb(0.039, 0.314, 0.620), pch = 18, cex = 2))
  text(c(0.6975, 0.745), c(0.0355, 0.009), c("Player A", "Player B"))
  dev.off()
}

{
  pdf("~/Downloads/conference_comparison.pdf", height = 5, width = 10)
  par(mfrow = c(1, 2))

  pg_overall_per_set |>
    dplyr::filter(conference_name == "Pac-12") |>
    with(
      hist(pg_raw,
        breaks = 40,
        col = rice_rich_blue_transparent,
        border = rice_rich_blue_transparent,
        main = "BEFORE Adjustment",
        xlab = "Points Gained per Set (all skills, min. 50 sets)",
        ylab = "Number of Players"
      )
    )
  pg_overall_per_set |>
    dplyr::filter(conference_name == "Summit League") |>
    with(hist(pg_raw, breaks = 40, col = rice_gray_transparent, border = rice_gray_transparent, add = TRUE))
  legend("topright",
    legend = c("Pac-12", "Summit League"),
    fill = c(rice_rich_blue_transparent, rice_gray_transparent),
    border = c(rice_rich_blue_transparent, rice_gray_transparent),
    bty = "n"
  )

  pg_overall_per_set |>
    dplyr::filter(conference_name == "Pac-12") |>
    with(
      hist(pg_adj,
        breaks = 40,
        col = rice_rich_blue_transparent,
        border = rice_rich_blue_transparent,
        main = "AFTER Adjustment",
        xlab = "ADJ Points Gained per Set (all skills, min. 50 sets)",
        ylab = "Number of Players"
      )
    )
  pg_overall_per_set |>
    dplyr::filter(conference_name == "Summit League") |>
    with(hist(pg_adj, breaks = 40, col = rice_gray_transparent, border = rice_gray_transparent, add = TRUE))
  legend("topright",
    legend = c("Pac-12", "Summit League"),
    fill = c(rice_rich_blue_transparent, rice_gray_transparent),
    border = c(rice_rich_blue_transparent, rice_gray_transparent),
    bty = "n"
  )

  dev.off()
}

pg_attack |>
  dplyr::filter(player_position == "OH", n >= 200) |>
  dplyr::mutate(adj = pg_adj - pg_raw) |>
  dplyr::group_by(team_name) |>
  dplyr::summarize(diff = diff(range(pg_adj - pg_raw))) |>
  dplyr::arrange(-diff)

pg_attack |>
  dplyr::filter(team_name == "Rutgers University", player_position == "OH", n >= 200) |>
  dplyr::mutate(adj = pg_adj - pg_raw) |>
  dplyr::arrange(adj) |>
  dplyr::select(player_id, player_name, pg_raw, pg_adj, adj, n)

attack_data_with_pg_adj |>
  dplyr::filter(player_id == -369172, player_position == "OH") |>
  dplyr::arrange(sos_offense) |>
  dplyr::select(blocker_id, digger_id, sos_offense, is_block_through)

{
  pdf("~/Downloads/teammate_comparison.pdf", height = 5, width = 5)
  attack_data_with_pg_adj |>
    dplyr::filter(player_id == -369172, player_position == "OH") |>  # Taylor Humphrey
    with(
      hist(-sos_offense,
        breaks = 20,
        col = rice_rich_blue_transparent,
        border = rice_rich_blue_transparent,
        main = "",
        xlab = "Strength of Schedule (points gained per attack)",
        ylab = "Number of Attacks",
        axes = FALSE
      )
    )
  attack_data_with_pg_adj |>
    dplyr::filter(player_id == -335306, player_position == "OH") |>  # Alissa Kinkela
    with(
      hist(-sos_offense,
        breaks = 20,
        col = rice_gray_transparent,
        border = rice_gray_transparent,
        add = TRUE
      )
    )
  legend("topright",
    legend = c("Player A", "Player B"),
    fill = c(rice_rich_blue_transparent, rice_gray_transparent),
    border = c(rice_rich_blue_transparent, rice_gray_transparent),
    bty = "n"
  )
  axis(1,
    at = seq(from = -0.15, to = 0.15, by = 0.05),
    labels = c("-15%", "-10%", "-5%", "0%", "+5%", "+10%", "+15%")
  )
  axis(2)
  dev.off()
}

pg_overall_per_set |>
  dplyr::group_by(conference_name) |>
  dplyr::summarize(pg_adj = weighted.mean(pg_adj, w = sets_played)) |>
  dplyr::arrange(-pg_adj) |>
  print(n = 30)

avca <- read.csv("R/avca.csv") |>
  dplyr::mutate(player_id = as.character(player_id))

pg_avca <- pg_overall_per_set |>
  dplyr::inner_join(avca, by = "player_id") |>
  dplyr::group_by(avca) |>
  dplyr::summarize(
    pg_raw = weighted.mean(pg_raw, w = sets_played),
    pg_adj = weighted.mean(pg_adj, w = sets_played)
  )

texas_orange <- rgb(191, 87, 0, maxColorValue = 255)
gold <- rgb(214, 175, 54, maxColorValue = 255)
silver <- rgb(167, 167, 173, maxColorValue = 255)
bronze <- rgb(167, 112, 68, maxColorValue = 255)
breaks <- seq(from = -2, to = 2, by = 0.05)

{
  pdf("~/Downloads/avca_all_americans_raw.pdf", height = 5, width = 6)
  par(mar = c(5.1, 2.1, 4.1, 0))
  pg_overall_per_set |>
    dplyr::filter(sets_played >= 50) |>
    with(
      hist(pg_raw,
        breaks = breaks,
        col = rice_blue_transparent,
        border = rice_blue_transparent,
        xlim = c(-0.7, 0.7),
        ylim = c(0, 400),
        main = "RAW Points Gained per Set",
        xlab = "",
        ylab = "",
        axes = FALSE
      )
    )
  abline(v = 0.565, lty = 2, col = texas_orange)
  abline(v = pg_avca$pg_raw, lwd = 4, lty = c(1, 1, 1, 3), col = c(gold, silver, bronze, "black"))
  legend("topleft",
    legend = c("Logan Eggleston", "All-Am. 1st Team", "All-Am. 2nd Team", "All-Am. 3rd Team", "Honorable Mention"),
    bty = "n",
    col = c(texas_orange, gold, silver, bronze, "black"),
    lwd = c(1, 4, 4, 4, 4),
    lty = c(2, 1, 1, 1, 3)
  )
  axis(1, at = c(-0.5, 0, 0.5))
  dev.off()
}

{
  pdf("~/Downloads/avca_all_americans_adj.pdf", height = 5, width = 8)
  par(mar = c(5.1, 2.1, 4.1, 0))
  pg_overall_per_set |>
    dplyr::filter(sets_played >= 50) |>
    with(
      hist(pg_adj,
        breaks = breaks,
        col = rice_blue_transparent,
        border = rice_blue_transparent,
        xlim = c(-0.6, 1),
        ylim = c(0, 400),
        main = "Adjusted Points Gained per Set",
        xlab = "",
        ylab = "",
        axes = FALSE
      )
    )
  abline(v = 0.888, lty = 2, col = texas_orange)
  abline(v = pg_avca$pg_adj, lwd = 4, lty = c(1, 1, 1, 3), col = c(gold, silver, bronze, "black"))
  legend("topleft",
    legend = c("Logan Eggleston", "All-Am. 1st Team", "All-Am. 2nd Team", "All-Am. 3rd Team", "Honorable Mention"),
    bty = "n",
    col = c(texas_orange, gold, silver, bronze, "black"),
    lwd = c(1, 4, 4, 4, 4),
    lty = c(2, 1, 1, 1, 3)
  )
  axis(1, at = c(-0.5, 0, 0.5, 1))
  dev.off()
}


power_five_conf <- c(2, 9, 11, 23, 25)
attack_data_with_pg_adj |>
  dplyr::filter(
    digger_id == defense_back_middle_player_id
  ) |>
  with(table(digger_resp))

power_five_data <- contact_sideout_prob |>
  dplyr::filter(conf_id_home %in% power_five_conf, conf_id_away %in% power_five_conf) |>
  dplyr::mutate(
    oh1_row_home = ifelse(home_setter_position %in% 4:6, "oh1_front", "oh1_back"),
    oh1_row_away = ifelse(visiting_setter_position %in% 4:6, "oh1_front", "oh1_back"),
    home_player_id_oh1 = dplyr::case_when(
      home_setter_position == 1 ~ home_player_id5,
      home_setter_position == 2 ~ home_player_id6,
      home_setter_position == 3 ~ home_player_id1,
      home_setter_position == 4 ~ home_player_id2,
      home_setter_position == 5 ~ home_player_id3,
      home_setter_position == 6 ~ home_player_id4
    ),
    away_player_id_oh1 = dplyr::case_when(
      visiting_setter_position == 1 ~ visiting_player_id5,
      visiting_setter_position == 2 ~ visiting_player_id6,
      visiting_setter_position == 3 ~ visiting_player_id1,
      visiting_setter_position == 4 ~ visiting_player_id2,
      visiting_setter_position == 5 ~ visiting_player_id3,
      visiting_setter_position == 6 ~ visiting_player_id4
    ),
    home_player_id_oh2 = dplyr::case_when(
      home_setter_position == 1 ~ home_player_id2,
      home_setter_position == 2 ~ home_player_id3,
      home_setter_position == 3 ~ home_player_id4,
      home_setter_position == 4 ~ home_player_id5,
      home_setter_position == 5 ~ home_player_id6,
      home_setter_position == 6 ~ home_player_id1
    ),
    away_player_id_oh2 = dplyr::case_when(
      visiting_setter_position == 1 ~ visiting_player_id2,
      visiting_setter_position == 2 ~ visiting_player_id3,
      visiting_setter_position == 3 ~ visiting_player_id4,
      visiting_setter_position == 4 ~ visiting_player_id5,
      visiting_setter_position == 5 ~ visiting_player_id6,
      visiting_setter_position == 6 ~ visiting_player_id1
    )
  )

p5_oh_data_home <- power_five_data |>
  dplyr::select(
    match_id, set_number, skill,
    team_id = home_team_id, oh1_row = oh1_row_home,
    player_id_oh1 = home_player_id_oh1, player_id_oh2 = home_player_id_oh2
  )
p5_oh_data_away <- power_five_data |>
  dplyr::select(
    match_id, set_number, skill,
    team_id = visiting_team_id, oh1_row = oh1_row_away,
    player_id_oh1 = away_player_id_oh1, player_id_oh2 = away_player_id_oh2
  )

p5_oh_data <- dplyr::bind_rows(p5_oh_data_home, p5_oh_data_away) |>
  dplyr::group_by(match_id, set_number, team_id, oh1_row) |>
  dplyr::filter(skill != "Serve") |>
  dplyr::count(player_id_oh1, player_id_oh2) |>
  dplyr::group_by(match_id, set_number, team_id, oh1_row) |>
  dplyr::arrange(-n) |>
  dplyr::select(-n) |>
  dplyr::slice(1) |>
  dplyr::ungroup() |>
  tidyr::pivot_longer(
    cols = dplyr::starts_with("player_id_oh"),
    names_to = "position",
    values_to = "player_id"
  ) |>
  dplyr::mutate(position = ifelse(grepl("1$", position), "OH1", "OH2")) |>
  tidyr::pivot_wider(names_from = oh1_row, values_from = player_id) |>
  dplyr::transmute(
    match_id, set_number, team_id, position,
    front = ifelse(position == "OH1", oh1_front, oh1_back),
    back = ifelse(position == "OH1", oh1_back, oh1_front),
    platoon = front != back
  ) |>
  tidyr::pivot_longer(cols = c("front", "back"), names_to = "row", values_to = "player_id")

p5_platoon_data <- p5_oh_data |>
  dplyr::left_join(pg_reception, by = "player_id") |>
  dplyr::left_join(pg_dig, by = "player_id") |>
  dplyr::mutate(
    n_reception = dplyr::coalesce(n_reception, 0),
    pg_reception_adj = dplyr::coalesce(pg_reception_adj, 0),
    n_dig = dplyr::coalesce(n_dig, 0),
    pg_dig_adj = dplyr::coalesce(pg_dig_adj, 0),
    n = n_reception + n_dig
  )

cutoff <- 30

p5_platoon_summary <- p5_platoon_data |>
  dplyr::group_by(platoon, row) |>
  dplyr::summarize(
    pg_reception_adj = weighted.mean(pg_reception_adj, w = n_reception >= cutoff),
    pct_missing_reception = mean(n_reception < cutoff),
    pg_dig_adj = weighted.mean(pg_dig_adj, w = n_dig >= cutoff),
    pct_missing_dig = mean(n_dig < cutoff),
    .groups = "drop"
  )

breaks <- seq(from = -1, to = 1, by = 0.005)

{
  pdf("~/Downloads/oh_comparison.pdf", height = 5, width = 6)
  p5_platoon_data |>
    dplyr::filter(n_reception >= cutoff, !platoon, row == "back") |>
    with(
      hist(
        x = pg_reception_adj,
        col = rice_rich_blue_transparent,
        border = rice_rich_blue_transparent,
        breaks = breaks,
        xlim = c(-0.07, 0.05),
        axes = FALSE,
        xlab = "Reception Points Gained per Opportunity (season-long average)",
        ylab = "Number of Player-Sets",
        main = "Reception"
      )
    )
  p5_platoon_data |>
    dplyr::filter(n_reception >= cutoff, platoon, row == "front") |>
    with(
      hist(
        pg_reception_adj,
        col = rice_gray_transparent,
        border = rice_gray_transparent,
        breaks = breaks,
        add = TRUE
      )
    )
  axis(1, at = c(-0.05, 0, 0.05), labels = c("-5%", "0%", "+5%"))
  axis(2)
  abline(
    v = p5_platoon_summary |>
      dplyr::filter(row == "front") |>
      with(pg_reception_adj),
    col = c(rice_rich_blue, rice_gray),
    lwd = 4,
    lty = 2
  )
  legend(
    x = "topleft",
    legend = c("All-Around OH", "Front-Only OH"),
    fill = c(rice_rich_blue_transparent, rice_gray_transparent),
    border = c(rice_rich_blue_transparent, rice_gray_transparent),
    bty = "n"
  )
  dev.off()
}

{
  pdf("~/Downloads/oh_comparison_with_ds.pdf", height = 5, width = 6)
  p5_platoon_data |>
    dplyr::filter(n_reception >= cutoff, !platoon, row == "back") |>
    with(
      hist(
        x = pg_reception_adj,
        col = rice_rich_blue_transparent,
        border = rice_rich_blue_transparent,
        breaks = breaks,
        xlim = c(-0.07, 0.05),
        axes = FALSE,
        xlab = "Reception Points Gained per Opportunity (season-long average)",
        ylab = "Number of Player-Sets",
        main = "Reception"
      )
    )
  p5_platoon_data |>
    dplyr::filter(n_reception >= cutoff, platoon, row == "front") |>
    with(
      hist(
        pg_reception_adj,
        col = rice_gray_transparent,
        border = rice_gray_transparent,
        breaks = breaks,
        add = TRUE
      )
    )
  p5_platoon_data |>
    dplyr::filter(n_reception >= cutoff, platoon, row == "back") |>
    with(
      hist(
        pg_reception_adj,
        col = rice_blue_transparent,
        border = rice_blue_transparent,
        breaks = breaks,
        add = TRUE
      )
    )
  axis(1, at = c(-0.05, 0, 0.05), labels = c("-5%", "0%", "+5%"))
  axis(2)
  abline(
    v = p5_platoon_summary |>
      dplyr::filter(!(row == "back" & !platoon)) |>
      with(pg_reception_adj),
    col = c(rice_rich_blue, rice_blue, rice_gray),
    lwd = 4,
    lty = 2
  )
  legend(
    x = "topleft",
    legend = c("All-Around OH", "Front-Only OH", "DS"),
    fill = c(rice_rich_blue_transparent, rice_gray_transparent, rice_blue_transparent),
    border = c(rice_rich_blue_transparent, rice_gray_transparent, rice_blue_transparent),
    bty = "n"
  )
  dev.off()
}

serve_data_adj |>
  dplyr::filter(conf_id_home %in% power_five_conf, conf_id_away %in% power_five_conf) |>
  dplyr::mutate(
    receiving_team_id = ifelse(team_id == home_team_id, visiting_team_id, home_team_id),
    receiving_setter_position = ifelse(team_id == home_team_id, visiting_setter_position, home_setter_position),
    receiving_player_id1 = ifelse(team_id == home_team_id, visiting_player_id1, home_player_id1),
    receiving_player_id2 = ifelse(team_id == home_team_id, visiting_player_id2, home_player_id2),
    receiving_player_id3 = ifelse(team_id == home_team_id, visiting_player_id3, home_player_id3),
    receiving_player_id4 = ifelse(team_id == home_team_id, visiting_player_id4, home_player_id4),
    receiving_player_id5 = ifelse(team_id == home_team_id, visiting_player_id5, home_player_id5),
    receiving_player_id6 = ifelse(team_id == home_team_id, visiting_player_id6, home_player_id6),
    back_row_oh_player_id = dplyr::case_when(
      receiving_setter_position == 1 ~ receiving_player_id5,  # OH1
      receiving_setter_position == 2 ~ receiving_player_id6,  # OH1
      receiving_setter_position == 3 ~ receiving_player_id1,  # OH1
      receiving_setter_position == 4 ~ receiving_player_id5,  # OH2
      receiving_setter_position == 5 ~ receiving_player_id6,  # OH2
      receiving_setter_position == 6 ~ receiving_player_id1   # OH2
    ),
    front_row_oh_player_id = dplyr::case_when(
      receiving_setter_position == 1 ~ receiving_player_id2,  # OH1
      receiving_setter_position == 2 ~ receiving_player_id3,  # OH1
      receiving_setter_position == 3 ~ receiving_player_id4,  # OH1
      receiving_setter_position == 4 ~ receiving_player_id2,  # OH2
      receiving_setter_position == 5 ~ receiving_player_id3,  # OH2
      receiving_setter_position == 6 ~ receiving_player_id4   # OH2
    )
  ) |>
  dplyr::count(match_id, set_number, team_id_defense) |>
  with(mean(n))

##big12 <- c(
##  126,  # Texas
##  118,  # Baylor
##  117,  # TCU
##  119,  # Iowa St
##  124,  # Kansas
##  122,  # Kansas St
##  123,  # Texas Tech
##  125,  # Oklahoma
##  120   # West Virginia
##)
##
##serve_data <- contact_sideout_prob |>
##  dplyr::filter(
##    home_team_id %in% big12 | visiting_team_id %in% big12
##  ) |>
##  dplyr::mutate(
##    opposing_team_id = ifelse(team_id == home_team_id, visiting_team_id, home_team_id),
##    player_id = ifelse(player_id == "unknown player", 0, player_id),
##    server = ifelse(team_id %in% big12, player_id, team_id),
##    receiver = ifelse(dplyr::lead(team_id, 1) %in% big12, dplyr::lead(player_id, 1), dplyr::lead(team_id, 1)),
##    zone = end_zone,
##    server_zone = paste(server, end_zone, sep = "_"),
##    pass_eval = dplyr::lead(evaluation_code, 1),
##    points_added = dplyr::lead(ifelse(skill == "serve", NA, c(NA, diff(sideout_prob))))
##  ) |>
##  dplyr::filter(skill == "Serve", !is.na(zone), !is.na(points_added), zone %in% c(1, 5, 6)) |>
##  dplyr::mutate(zone = factor(zone, levels = c(6, 1, 5)))
##
##variable <- serve_data |>
##  with(c(paste0("z_", zone), paste0("s_", server), paste0("r_", receiver), paste0("sz_", server, "_", zone)))
##
##design_matrix <- Matrix::sparseMatrix(
##  i = rep(1:nrow(serve_data), times = 4),
##  j = as.integer(as.factor(variable))
##)
##
##fit <- lme4::lmer(points_added ~ zone + (1 + zone | server) + (1 | receiver), data = serve_data)
##
##player <- plays |>
##  dplyr::filter(!is.na(player_id), player_id != "unknown player") |>
##  dplyr::distinct(player_id, player_name, team_id)
##
##team <- plays |>
##  dplyr::filter(!is.na(team_id)) |>
##  dplyr::mutate(team_id = as.character(team_id)) |>
##  dplyr::distinct(team_id, team)
##
###receiver <- coef |>
###  dplyr::filter(substring(variable, 1, 1) == "r") |>
###  dplyr::mutate(player_id = substring(variable, 3)) |>
##receiver <- lme4::ranef(fit)$receiver |>
##  tibble::rownames_to_column("player_id") |>
##  dplyr::rename(coef = `(Intercept)`) |>
##  dplyr::left_join(player, by = "player_id") |>
##  dplyr::left_join(team, by = c("player_id" = "team_id")) |>
##  tibble::as_tibble()
##
##ace_pct <- serve_data |>
##  dplyr::group_by(player_id) |>
##  dplyr::summarize(n = dplyr::n(), ace_pct = mean(evaluation_code == "#"))
##
##zone <- lme4::fixef(fit) |>
##  t() |>
##  tibble::as_tibble() |>
##  dplyr::transmute(z6 = `(Intercept)`, z1 = `(Intercept)` + zone1, z5 = `(Intercept)` + zone5) |>
##  tidyr::pivot_longer(cols = dplyr::starts_with("z"), names_to = "zone", values_to = "intercept") |>
##  dplyr::mutate(
##    zone = as.integer(substring(zone, 2)),
##    intercept = intercept - mean(intercept)
##  )
##
##server <- lme4::ranef(fit)$server |>
##  tibble::rownames_to_column("player_id") |>
##  dplyr::transmute(player_id, z6 = `(Intercept)`, z1 = `(Intercept)` + zone1, z5 = `(Intercept)` + zone5) |>
##  tidyr::pivot_longer(cols = dplyr::starts_with("z"), names_to = "zone", values_to = "server_effect") |>
##  dplyr::mutate(zone = as.integer(substring(zone, 2))) |>
##  dplyr::left_join(zone, by = "zone") |>
##  dplyr::left_join(player, by = "player_id") |>
##  dplyr::left_join(team, by = c("player_id" = "team_id")) |>
##  dplyr::left_join(ace_pct, by = "player_id") |>
##  dplyr::mutate(pred_points_added = intercept + server_effect)
##

# Bootstrap standard errors ----

folders <- list.files("R/bootstrap")

conference <- NULL
for (folder in folders) {
  conference_files <- list.files(glue::glue("R/bootstrap/{folder}/conference"))
  for (file in conference_files) {
    conference <- dplyr::bind_rows(
      conference,
      data.table::fread(glue::glue("R/bootstrap/{folder}/conference/{file}")) |>
        tibble::add_column(boot = paste(folder, file, sep = "/"), .before = 1)
    )
  }
}
conference_se <- conference |>
  dplyr::group_by(conference_name) |>
  dplyr::summarize(se = sd(pg_adj), .groups = "drop") |>
  as.data.frame()

division <- NULL
for (folder in folders) {
  division_files <- list.files(glue::glue("R/bootstrap/{folder}/division"))
  for (file in division_files) {
    division <- dplyr::bind_rows(
      division,
      data.table::fread(glue::glue("R/bootstrap/{folder}/division/{file}")) |>
        tibble::add_column(boot = paste(folder, file, sep = "/"), .before = 1)
    )
  }
}
division_se <- division |>
  dplyr::select(-boot) |>
  dplyr::summarize_all(~ sd(.))

pg_overall <- NULL
for (folder in folders) {
  pg_overall_files <- list.files(glue::glue("R/bootstrap/{folder}/pg_overall"))
  for (file in pg_overall_files) {
    pg_overall <- dplyr::bind_rows(
      pg_overall,
      data.table::fread(glue::glue("R/bootstrap/{folder}/pg_overall/{file}")) |>
        tibble::add_column(boot = paste(folder, file, sep = "/"), .before = 1)
    )
  }
}
pg_overall_se <- pg_overall |>
  dplyr::select(-boot) |>
  dplyr::group_by(player_id, player_name, team_name, conference_name) |>
  dplyr::summarise_all(~ sd(.))
