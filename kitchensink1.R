if (!require("pacman")) install.packages("pacman")
pacman::p_load(rjson, plyr, tm, h2o, text2vec, data.table, doParallel, SoundexBR, SnowballC, gsubfn, tau, optparse, digest, Matrix, caret)

nodes <- detectCores()
cl <- makeCluster(nodes)
registerDoParallel(cl)

legis <- fromJSON(file="camara_sp_downloads/data/legis.json")

# dest <- tempfile(fileext = ".pdf")
# download.file(url, dest, mode = "wb")

# pdf <- readPDF(control = list(text = "-layout"))(elem = list(uri = "http://cmspbdoc.inf.br/iah/fulltext/projeto/PL0432-2009.pdf"),
#                                                  language = "en",
#                                                  id = "id1")

ldply(legis, function(x) {
  if ("url_pdf" %in% names(x) & tolower(x$tipo) == "pl" & "encerramento" %in% names(x)) {
    tmp_path <- paste0("tmp/", x$`_id`, ".pdf")
    if (!file.exists(tmp_path)) {
      download.file(x$url_pdf, destfile = tmp_path)
    }
  }
}, .parallel=TRUE)

autores2012 <- read.csv(file="camara_sp_downloads/autores.csv", header = FALSE)

df_legis <- ldply(legis, function(x) {
  if ("url_pdf" %in% names(x) & tolower(x$tipo) == "pl" & "encerramento" %in% names(x)) {
    autores2012 <- read.csv(file="camara_sp_downloads/autores.csv", header = FALSE)
    
    file <- paste0("tmp/", x$`_id`, ".pdf")
    fonts <- system(paste0("pdffonts ", file), intern = TRUE)
    is_text <- NROW(fonts) > 2
    
    if (is_text) {
      data.frame(text=paste(system(paste0("pdftotext ", file, " -"), intern = TRUE), collapse = ' '),
                 comissoes=paste(x$comissoes, collapse = ' '),
                 assuntos=paste(x$assuntos, collapse = ' '),
                 autores=paste(x$autores, collapse = ' '),
                 ementa=paste(x$ementa, collapse = ' '),
                 ano=x$ano,
                 qtd_votos=sum(autores2012[autores2012$V1 %in% x$autores, "V2"]),
                 encerramento=x$encerramento
                 )
#       TODO: apply OCR below
#     } else {
#       data.frame(id=x$`_id`,
#                  is_text=is_text,
#                  text="",
#                  encerramento=x$encerramento)
    }
  }
}, .parallel = TRUE)

df_legis$promulgado <- df_legis$encerramento == "PROMULGADO"

toSpace <- content_transformer(function (x , pattern ) gsub(pattern, " ", x))

df_legis$text <- accent(df_legis$text)
df_legis$comissoes <- accent(df_legis$comissoes)
df_legis$assuntos <- accent(df_legis$assuntos)
df_legis$autores <- accent(df_legis$autores)
df_legis$ementa <- accent(df_legis$ementa)

df_legis$text <- gsub("[^a-zA-Z0-9 ]", "", df_legis$text)
df_legis$comissoes <- gsub("[^a-zA-Z0-9 ]", "", df_legis$comissoes)
df_legis$assuntos <- gsub("[^a-zA-Z0-9 ]", "", df_legis$assuntos)
df_legis$autores <- gsub("[^a-zA-Z0-9 ]", "", df_legis$autores)
df_legis$ementa <- gsub("[^a-zA-Z0-9 ]", "", df_legis$ementa)

# Load the data as a corpus
BigramTokenizer <- function(x) RWeka::NGramTokenizer(x, RWeka::Weka_control(min = 2, max = 2))

text_corpus <- VCorpus(VectorSource(df_legis$text),  readerControl = list(language = "portuguese"))
text_corpus <- tm_map(text_corpus, toSpace, "/")
text_corpus <- tm_map(text_corpus, toSpace, "@")
text_corpus <- tm_map(text_corpus, toSpace, "\\|")
text_corpus <- tm_map(text_corpus, content_transformer(tolower))
text_corpus <- tm_map(text_corpus, removeNumbers)
text_corpus <- tm_map(text_corpus, removeWords, stopwords("portuguese"))
text_corpus <- tm_map(text_corpus, removePunctuation)
text_corpus <- tm_map(text_corpus, stripWhitespace)
text_corpus <- tm_map(text_corpus, stemDocument, mc.cores=1)

# Create the Document-Term matrix
text_dtm <- DocumentTermMatrix(text_corpus, control = list(bounds = list(global = c(0, Inf)))) 
dim(text_dtm)
text_matrix <- as.matrix(text_dtm)
colnames(text_matrix) <- paste("text", colnames(text_matrix), sep = "_")

comissoes_corpus <- VCorpus(VectorSource(df_legis$comissoes),  readerControl = list(language = "portuguese"))
comissoes_corpus <- tm_map(comissoes_corpus, toSpace, "/")
comissoes_corpus <- tm_map(comissoes_corpus, toSpace, "@")
comissoes_corpus <- tm_map(comissoes_corpus, toSpace, "\\|")
comissoes_corpus <- tm_map(comissoes_corpus, content_transformer(tolower))
comissoes_corpus <- tm_map(comissoes_corpus, removeNumbers)
comissoes_corpus <- tm_map(comissoes_corpus, removeWords, stopwords("portuguese"))
comissoes_corpus <- tm_map(comissoes_corpus, removePunctuation)
comissoes_corpus <- tm_map(comissoes_corpus, stripWhitespace)

comissoes_dtm <- DocumentTermMatrix(comissoes_corpus, control = list(bounds = list(global = c(0, Inf)))) 
dim(comissoes_dtm)
comissoes_matrix <- as.matrix(comissoes_dtm)
colnames(comissoes_matrix) <- paste("comissoes", colnames(comissoes_matrix), sep = "_")

assuntos_corpus <- VCorpus(VectorSource(df_legis$assuntos),  readerControl = list(language = "portuguese"))
assuntos_corpus <- tm_map(assuntos_corpus, toSpace, "/")
assuntos_corpus <- tm_map(assuntos_corpus, toSpace, "@")
assuntos_corpus <- tm_map(assuntos_corpus, toSpace, "\\|")
assuntos_corpus <- tm_map(assuntos_corpus, content_transformer(tolower))
assuntos_corpus <- tm_map(assuntos_corpus, removeNumbers)
assuntos_corpus <- tm_map(assuntos_corpus, removeWords, stopwords("portuguese"))
assuntos_corpus <- tm_map(assuntos_corpus, removePunctuation)
assuntos_corpus <- tm_map(assuntos_corpus, stripWhitespace)

assuntos_dtm <- DocumentTermMatrix(assuntos_corpus, control = list(bounds = list(global = c(0, Inf)))) 
dim(assuntos_dtm)
assuntos_matrix <- as.matrix(assuntos_dtm)
colnames(assuntos_matrix) <- paste("assuntos", colnames(assuntos_matrix), sep = "_")

autores_corpus <- VCorpus(VectorSource(df_legis$autores),  readerControl = list(language = "portuguese"))
autores_corpus <- tm_map(autores_corpus, toSpace, "/")
autores_corpus <- tm_map(autores_corpus, toSpace, "@")
autores_corpus <- tm_map(autores_corpus, toSpace, "\\|")
autores_corpus <- tm_map(autores_corpus, content_transformer(tolower))
autores_corpus <- tm_map(autores_corpus, removeNumbers)
autores_corpus <- tm_map(autores_corpus, removeWords, stopwords("portuguese"))
autores_corpus <- tm_map(autores_corpus, removePunctuation)
autores_corpus <- tm_map(autores_corpus, stripWhitespace)

# autores_dtm <- DocumentTermMatrix(autores_corpus, control=list(tokenize=BigramTokenizer, bounds = list(global = c(0, Inf))))
autores_dtm <- DocumentTermMatrix(autores_corpus, control=list(bounds = list(global = c(0, Inf))))
dim(autores_dtm)
autores_matrix <- as.matrix(autores_dtm)
colnames(autores_matrix) <- paste("autores", colnames(autores_matrix), sep = "_")

ementa_corpus <- VCorpus(VectorSource(df_legis$ementa),  readerControl = list(language = "portuguese"))
ementa_corpus <- tm_map(ementa_corpus, toSpace, "/")
ementa_corpus <- tm_map(ementa_corpus, toSpace, "@")
ementa_corpus <- tm_map(ementa_corpus, toSpace, "\\|")
ementa_corpus <- tm_map(ementa_corpus, content_transformer(tolower))
ementa_corpus <- tm_map(ementa_corpus, removeNumbers)
ementa_corpus <- tm_map(ementa_corpus, removeWords, stopwords("portuguese"))
ementa_corpus <- tm_map(ementa_corpus, removePunctuation)
ementa_corpus <- tm_map(ementa_corpus, stripWhitespace)

ementa_dtm <- DocumentTermMatrix(ementa_corpus, control = list(bounds = list(global = c(0, Inf)))) 
dim(ementa_dtm)
ementa_matrix <- as.matrix(ementa_dtm)
colnames(ementa_matrix) <- paste("ementa", colnames(ementa_matrix), sep = "_")

df_legis_final <- cbind(df_legis, as.data.frame(text_matrix), as.data.frame(assuntos_matrix), as.data.frame(autores_matrix), as.data.frame(ementa_matrix), as.data.frame(comissoes_matrix))

df_legis_light <- cbind(df_legis, as.data.frame(assuntos_matrix), as.data.frame(comissoes_matrix))
df_legis_light$encerramento <- NULL

# text_corpus.copy <- text_corpus
# text_corpus <- tm_map(text_corpus, stemCompletion, dictionary=text_corpus.copy)

# content(text_corpus[[1]])

# inTraining <- createDataPartition(df_legis_final$promulgado, p = .75, list = FALSE)
# training <- df_legis_final[ inTraining,]
# testing  <- df_legis_final[-inTraining,]

inTraining <- createDataPartition(df_legis_light$promulgado, p = .75, list = FALSE)
training <- df_legis_light[ inTraining,]
testing  <- df_legis_light[-inTraining,]

# inTraining <- createDataPartition(training$promulgado, p = .85, list = FALSE)
# training <- df_legis_final[ inTraining,]
# validation  <- df_legis_final[-inTraining,]

localH2O <- h2o.init(ip = "localhost", port = 54321, startH2O = TRUE, max_mem_size = '4g', nthreads = -1)

train_h2o <- as.h2o(training)
test_h2o <- as.h2o(testing)
# validate_h2o <- as.h2o(validation)

trainingAndTestingGbm50 <- h2o.gbm(x = setdiff(names(df_legis_light), c("promulgado")),
                                                y = 'promulgado',
                                                training_frame = train_h2o,
                                                validation_frame = test_h2o,
                                                model_id = "trainingAndTestingGbm50",
                                                # distribution = 'multinomial',
                                                ntrees = 50,
                                                max_depth = 10,
                                                min_rows = 20)

h2o.auc(h2o.performance(trainingAndTestingGbmWithoutLabels50, test_h2o))

h2o.saveModel(trainingAndTestingGbmWithoutLabels50, paste0(getwd(), "/analysis/kitchensink2/models"), force = TRUE)

h2o.shutdown()

stopCluster(cl)
