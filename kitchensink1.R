if (!require("pacman")) install.packages("pacman")
pacman::p_load(rjson, plyr, tm, h2o)

legis <- fromJson(file="camara_sp_downloads/data/legis.json")

df_legis <- ldply(legis, function(x) {
  data.frame()
})
