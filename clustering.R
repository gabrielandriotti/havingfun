library(dplyr)
library(readr)
library(clusterSim)
library(rgl)
library(cluster)
library(LICORS)
library(scatterplot3d)
library(car)


nomes_colunas <- c('ID','P1','P2','CARTAO_CRED','CARTAO_DEB','POUPANCA','OUTROS_INV','EMPRESTIMO','PAGAMENTO','TRANSFERENCIA',
'CHEQUE_ESPECIAL','OUTROS','P3','P4','PG_TARIFA_CESTA','PG_ANUIDADE','PG_TRANSFERENCIA','PG_SAQUE','PG_FALAR_GERENTE',
'PG_NAO_SABE','PG_OUTROS','P5','P6','P7','P8','P9','P10','P11','P12','IDADE','SEXO','CIDADE_VIVE','ESTADO','ATIVIDADE')


dados <- read_delim("dados_r.csv", 
                    delim = ";", quote = "", trim_ws = TRUE, col_names = nomes_colunas, 
                    col_types = cols_only(ID = "i", P1 = "c", P2 = "c", CARTAO_CRED = "d",
                    CARTAO_DEB = "d", POUPANCA = "d", OUTROS_INV = "d", EMPRESTIMO = "d",
                    PAGAMENTO = "d", TRANSFERENCIA = "d", CHEQUE_ESPECIAL = "d", OUTROS = "d",
                    P3 = "c", P4 = "c", PG_TARIFA_CESTA = "d", PG_ANUIDADE = "d",
                    PG_TRANSFERENCIA = "d", PG_SAQUE = "d", PG_FALAR_GERENTE = "d", PG_NAO_SABE = "d", PG_OUTROS = "d",
                    P5 = "c", P6 = "c", P7 = "c", P8 = "c",
                    P9 = "c", P10 = "c", P11 = "c", P12 = "c", IDADE = "i",
                    SEXO = "c", CIDADE_VIVE = "c", ESTADO = "c", ATIVIDADE = "c"), 
                    locale = locale(date_names = "pt", date_format = "%Y%m%d", decimal_mark = ","), na = c("", " ", "NA"))


# dados <- mutate(dados, IDADE_FAIXAS = cut(IDADE, breaks = c(18, 35, 60, Inf), right = FALSE, 
#                                           labels = c("18-35 anos", "35-60 anos", "60-100 anos")))



transform_cat <- function(dados, idvar = "ID", categvar, quantivar1, quantivar2, func1 = sum, func2 = mean) {
  out <- mapply(function(dados, id, categ) {
    out <- reshape(as.data.frame(table(dados[c(id, categ)])), v.names = "Freq", idvar = id, direction = "wide", timevar = categ)
    colnames(out) <- gsub("Freq", categ, colnames(out))
    out
  }, categ = categvar, MoreArgs = list(id = idvar, dados = dados))
  out2 <- aggregate(dados[, quantivar1], by = structure(list(ID = dados[,idvar]), .Names = idvar), FUN = func1)
  colnames(out2) <- c(idvar, quantivar1)
  out3 <- aggregate(dados[, quantivar2], by = structure(list(ID = dados[,idvar]), .Names = idvar), FUN = func2)
  colnames(out3) <- c(idvar, quantivar2)
  out4 <- merge(out2, out3)
  return(Reduce(function(x, y) merge(x, y, all = T), c(out, list(out4))))
}

dados <- as.data.frame(dados)
dados_transformados <- transform_cat(dados, idvar = "ID", 
                                   categvar = c('P1','P2','P5','P6','P7','P9','P11','P12'), 
                        quantivar1 = c(), 
                        quantivar2 = c(),
                        func1 = max, func2 = max)
# A ORDEM IMPORTA MUITO PARA DEPOIS CRIAR VARS NOVAS A PARTIR DOS DADOS
dados_transformados <- arrange(dados_transformados, ID)




g1 <- dados_transformados[-c(1)]

g1_norm <- data.Normalization(g1, type = "n0") #padronizar (n1)     de 0 a 1 (n4)       nada (n0)
d <- daisy(g1_norm, metric = "euclidean", stand = FALSE) 
fit <- cmdscale(d, eig = TRUE, k = 3)
x <- fit$points[, 1]
y <- fit$points[, 2]
z <- fit$points[, 3]



 wss <- (nrow(g1_norm)-1)*sum(apply(g1_norm,2,var))
 for (i in 2:15)
   wss[i] <- sum(kmeans(g1_norm,centers=i,nstart=50)$withinss)
   plot(1:15, wss, main="Metodo Elbow para descobrir o melhor numero de grupos (k)",
                 ylab="Soma dos quadrados de dentro dos grupos", xlab="Numero de grupos",
                 pch=20, cex=2, type="b")


fit2 = kmeanspp(g1_norm, k = 5, start = "random", iter.max = 100, nstart = 50, algorithm = "Hartigan-Wong")
# colors <- c("#B22222", "#E69F00", "#56B4E9", "#3CB371")
# colors <- colors[as.numeric(fit2$cluster)]
# scatterplot3d(x,y,z,main='Clusterizacao', pch = 16, color=colors)


scatter3d(x, y, z, groups = as.factor(fit2$cluster), grid=F, fit = "smooth", surface=F, sphere.size=1.4, ellipsoid.alpha=0.3, ellipsoid=T, labels = dados$ID, id.n=nrow(dados))
scatter3d(x, y, z, groups = as.factor(fit2$cluster), grid=F, fit = "smooth", surface=F, sphere.size=1.4, ellipsoid.alpha=0.3, ellipsoid=T)

#scatter3d(x, y, z, groups = as.factor(dados$SEXO), fit = "smooth", surface=F)
#scatter3d(x, y, z, groups = as.factor(dados$IDADE_FAIXAS), fit = "smooth", surface=F)
#scatter3d(x, y, z, groups = as.factor(dados$IDADE_FAIXAS), fit = "smooth", surface=F, labels = dados$ID, id.n=nrow(dados))


dados_transformados <- mutate(dados_transformados, CLUSTER = fit2$cluster)
output <- dplyr::select(dados_transformados, ID,CLUSTER)

write.csv(output, file='output.csv')


