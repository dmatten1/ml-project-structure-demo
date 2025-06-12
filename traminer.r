library(TraMineR)
library(TraMineRextras)
library(cluster)
library(WeightedCluster)
library(FactoMineR)
library(ade4)
library(RColorBrewer)
library(questionr)
library(descriptio)
library(dplyr)
library(purrr)
library(ggplot2)
library(seqhandbook)
library(tidyverse)

transcripts <- read.csv("Desktop/ncerdc_ml/ml-project-structure-demo/data/processed/transcripts_master.csv")
gifted <- read.csv("Desktop/ncerdc_ml/ml-project-structure-demo/data/processed/masterbuild_master.csv")


# First, create a list of non-gifted mastids from the `gifted` dataframe
non_gifted_ids <- gifted %>% #now gifted
  filter(aig != "N") %>%
  pull(mastid)

# Then filter your transcripts to include only those students
pivoted <- transcripts %>%
  filter(mastid %in% non_gifted_ids) %>%
  filter(!is.na(academic_level_desc), academic_level_desc != "") %>%
  group_by(mastid) %>%
  summarise(levels = list(academic_level_desc)) %>%
  ungroup() %>%
  filter(lengths(levels) >= 20)  # Keep only students with at least 20 classes








#pivoted_filtered <- pivoted |>
 # filter(mastid %in% gifted$mastid)

############traminer
set.seed(123)  # for reproducibility
sampled <- pivoted %>% sample_n(1000)


# Convert list-column to wide with one column per sequence position
seq_data <- sampled %>%
  mutate(id = row_number()) %>%
  tidyr::unnest_wider(levels, names_sep = "_") %>%
  select(-id)

seq_data_clean <- seq_data %>%
  mutate(across(everything(), ~ gsub("-", "_", .)))

seq_data_clean <- seq_data_clean |>
  select(-mastid)

states <- c(
  "Advanced Placement",
  "Co-op Education",
  "Honors/Advanced/Academically Gifted",
  "International Baccalaureate",
  "Non-Classroom Activity",
  "Standard Version",
  "Modified Curriculum",
  "Abridged/Adapted (Remedial)"
)

# 8 corresponding colors
colors <- c("red", "blue", "green", "orange", "purple", "yellow", "brown", "pink")

color_map <- setNames(colors, states)
transcripts$academic_level_color <- color_map[transcripts$academic_level_desc]

seq_obj_temp <- seqdef(seq_data_clean)
# Get true state order in sequence object
state_order <- attr(seq_obj_temp, "alphabet")  # same as seqstatd(seq_obj)$State

# Reorder colors accordingly
colors_ordered <- color_map[state_order]

# Convert to sequence object
seq_obj <- seqdef(seq_data_clean, states = state_order, cpal = colors_ordered)

# Example: frequency plot of states
seqIplot(seq_obj)

# Or compute distances, clusters, etc.



#CLUSTERING

costmatrix <- seqsubm(seq_obj, 
                      method = "TRATE", 
                      with.missing = TRUE, 
                      miss.cost = 0, 
                      transition = "both")
dist_omspell <- seqdist(seq_obj, method = "OMspell", sm = costmatrix, with.missing = TRUE)
#trying omspell above
clusterward1 <- agnes(dist_omspell, diss = TRUE, method = "ward")
plot(clusterward1, which.plot = 2)
#i chose 3
cl3 <- cutree(clusterward1, k = 4) 

#turning cut points into a factor variable and labeling them
cl3fac <- factor(cl3, labels = paste("Type", 1:4)) 

#plot
seqplot(seq_obj, group = cl3fac, type="I", sortv = "from.start",with.legend = TRUE, border = NA)

###########################################

sampled$cl3 <- cl3

gifted_sample <- sampled %>%
  select(mastid, cl3) %>%
  left_join(gifted, by = "mastid")  # brings in 'eds', 'sex', etc.


# Define the Big Five traits
traits <- c("eds")

gifted_sample <- gifted_sample %>%
  mutate(
    eds = ifelse(eds == "Y", 1,
                 ifelse(eds == "N", 0, NA))  # NA if value is not Y or N
  )
gifted_sample <- gifted_sample %>%
  mutate(sex = factor(sex, levels = c("M", "F")))

# Loop through traits and run ANOVA and TukeyHSD
for (trait in traits) {
  cat("\n============================\n")
  cat("Trait:", trait, "\n")
  cat("============================\n")
  
  # Build formula dynamically
  formula <- as.formula(paste(trait, "~ factor(cl3)"))
  
  # Run ANOVA
  aov_result <- aov(formula, data = gifted_sample)
  print(summary(aov_result))
  
  # Post-hoc test
  print(TukeyHSD(aov_result))
}

# Chi-squared test: association between sex and cluster
chisq_result <- chisq.test(table(gifted_sample$sex, gifted_sample$cl3))

# View the result
chisq_result
table(gifted_sample$sex, gifted_sample$cl3)
#Save

install.packages(c("gridExtra", "ggplot2", "knitr"))
library(gridExtra)
library(ggplot2)
library(knitr)

