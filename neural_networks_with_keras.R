devtools::install_github("rstudio/keras")
library(keras)
install_keras()
library(tidyverse)
library(readr)
library(dplyr)
library(magrittr)
library(scales)
library(caret)
library(ROCR)
library(ModelMetrics)
library(mice)


test <- read_csv('test_data.csv')
train <- read_csv('training_data.csv')

# calcula as qtdes de default e nao default
n_def0 <- train %>% filter(DEFAULT==0) %>% count() %>% pull(n)
n_def1 <- train %>% filter(DEFAULT==1) %>% count() %>% pull(n)

# balanceia a amostra em bons e maus
perc_maus <- 0.15 # colocar aqui o percentual de maus que se deseja no balanceamento
# 1 mau para (1/perc_maus-1) bons
train %<>% 
  filter(DEFAULT==1 | (DEFAULT==0 & runif(nrow(train))<=((1/perc_maus-1)*n_def1/n_def0)))


# transforma as categoricas em dummies
train %<>% 
  mutate(valor = 1) %>%
  spread(CATEGORICA1, valor, fill=0, sep='_') %>%
  mutate(valor = 1) %>%
  spread(CATEGORICA2, valor, fill=0, sep='_')

test %<>% 
  mutate(valor = 1) %>%
  spread(CATEGORICA1, valor, fill=0, sep='_') %>%
  mutate(valor = 1) %>%
  spread(CATEGORICA2, valor, fill=0, sep='_')


# IMPUTAR OS NAs DE VARIAVEIS ESPECIFICAS COM ZEROS
 train %<>%
   mutate_at(vars(starts_with('VARIAVEL_')),funs(replace(., is.na(.) == T, 0))) %>%
   mutate_at(vars(starts_with('VLR_')),funs(replace(., is.na(.) == T, 0)))
 
 test %<>%
   mutate_at(vars(starts_with('VARIAVEL_')),funs(replace(., is.na(.) == T, 0))) %>%
   mutate_at(vars(starts_with('VLR_')),funs(replace(., is.na(.) == T, 0)))



# retirar NAs em TRAIN DE VARIAVEIS ESPECIFICAS
train %<>%
  mutate(qtde_na=rowSums(is.na(select(.,starts_with('VAR_'))))) %>% 
  filter(qtde_na==0) %>%
  select(-qtde_na)


#prop.table(table(is.na(train)))


# imputar missing data com media de acordo com grupos
t_def1 <- train %>% filter(DEFAULT==1)
t_def0 <- train %>% filter(DEFAULT==0)
temp_t_def1 <- mice(t_def1,m=1,maxit=5,meth='mean',seed=500)
temp_t_def0 <- mice(t_def0,m=1,maxit=5,meth='mean',seed=500)
t_def1 <- complete(temp_t_def1,1)
t_def0 <- complete(temp_t_def0,1)
t_def1 %<>% mutate_all(funs(replace(., is.na(.) == T, 0)))
train <- bind_rows(t_def1,t_def0)

#prop.table(table(is.na(train)))
#prop.table(table(is.na(test)))


# retirar registros com NAs em TEST DE VARIAVEIS ESPECIFICAS
test %<>%
  mutate(qtde_na=rowSums(is.na(select(.,starts_with('VAR_'))))) %>% 
  filter(qtde_na==0) %>%
  select(-qtde_na)

#prop.table(table(is.na(test)))


# retira colunas com desvio padrao zero
test <- test[, sapply(train, function(x) {ifelse(is.character(x),T,sd(x,na.rm=T)!=0)} )]
train <- train[, sapply(train, function(x) {ifelse(is.character(x),T,sd(x,na.rm=T)!=0)} )]



# retira o id e a var dependente dos dados de treino
x_train <- train %>%
  select(-c(CPF,anomes,DEFAULT)) %>%
  as.matrix()

# faz o scaling dos dados de treino
x_train <- predict(preProcess(x_train, method = c('center','scale')), x_train)

# retira o id e a var dependente dos dados de teste
x_test <- test %>%
  select(-c(CPF,anomes,DEFAULT)) %>%
  as.matrix()

# faz o scaling dos dados de teste
x_test <- predict(preProcess(x_test, method = c('center','scale')), x_test)

# separa a var dependente nos dados de treino e teste
y_train <- train$DEFAULT %>% as.matrix()
y_test <- test$DEFAULT %>% as.matrix()


  
# cria a rede neural e estabelece a arquitetura
model <- keras_model_sequential() 
model %>% 
  layer_dense(units = 512, activation = 'relu', input_shape = c(ncol(x_train))) %>%
  layer_dropout(rate = 0.4) %>%
  layer_dense(units = 512, activation = 'relu') %>% 
  layer_dropout(rate = 0.4) %>%
  layer_dense(units = 64, activation = 'relu') %>% 
  layer_dropout(rate = 0.3) %>%
  layer_dense(units = 1, activation = 'sigmoid') 


# cria functions de acuracia e perda com threshold especifico
K <- backend()
accuracy_12 <- function(y_true, y_pred) {
  threshold = 0.12
  y_pred = K$cast(K$greater(y_pred, threshold), K$floatx())
  return(K$mean(K$equal(y_true, y_pred)))
}

# define a funcao perda e os parametros de otimizacao (https://keras.rstudio.com/articles/backend.html)
model %>% compile(
  loss = 'binary_crossentropy',
  #optimizer='rmsprop'
  optimizer = optimizer_adam(lr = 0.01, beta_1 = 0.9, beta_2 = 0.999, epsilon = 1e-08, decay = 0.001),
  metrics = c('ACC_12' = accuracy_12))

# define os parametros de execucao e roda o treino do modelo
history <- model %>% fit(
  x_train, y_train, 
  epochs = 100, batch_size = 256, 
  validation_data=list(x_test, y_test))

# validacao
test %<>%
  mutate(prev_prob=predict_proba(model,x_test))

real <- y_test
predito <- test$prev_prob

# plota curva ROC
pred <- prediction(predito,real)
perf <- performance(pred,"tpr","fpr")
plot(perf)

# calcula AUC
auc(real,predito)

# calcula ks
max(attr(perf,'y.values')[[1]]-attr(perf,'x.values')[[1]])
#ks.test(test$prev_prob[test$DEFAULT==1],test$prev_prob[test$DEFAULT==0])


# encontrar melhor cutoff
f1 <- array()
f2 <- array()
mcc <- array()
acc <- array()
inadimplencia <- array()
aprovacao <- array()
tpr <- array()
tnr <- array()
cuts <- seq(0,100,1)
for(i in 1:length(cuts)){
  mat <- confusionMatrix(real,predito,(i-1)/100)
  tp <- mat[2,2]
  tn <- mat[1,1]
  fp <- mat[2,1]
  fn <- mat[1,2]
  n <- sum(mat)
  s <- (tp+fn)/n
  p <- (tp+fp)/n
  acc[i] <- (tn+tp)/n
  mcc[i] <- (tp/n-s*p)/sqrt(p*s*(1-s)*(1-p))
  prec <- precision(real,predito,(i-1)/100)
  rec <- recall(real,predito,(i-1)/100)
  f1[i]  <- (1+1**2)*prec*rec/(1**2*prec+rec)
  f2[i]  <- (1+2**2)*prec*rec/(2**2*prec+rec)
  tnr[i] <- tn/(tn+fp)
  tpr[i] <- rec
  inadimplencia[i] <- fn/(fn+tn)
  aprovacao[i] <- (fn+tn)/n
}
cutsdf_nn <- data.frame(cuts,acc,mcc,f1,f2,inadimplencia,aprovacao,tnr,tpr)
cutsdf_nn %<>% gather(key=metrica, value=valor, acc,mcc,f1,f2,inadimplencia,aprovacao,tnr,tpr, na.rm=T)

ggplot(cutsdf_nn, aes(x=cuts, y=valor, color=metrica)) +
  geom_line()+
  scale_x_continuous(breaks = seq(from=0,to=100,by=5), name='% cutoff') +
  scale_y_continuous(breaks = seq(from=0,to=1,by=0.05), name='valor') +
  scale_color_manual(values=c('pink','blue','orange','green','red','black','yellow','brown')) +
  theme_minimal()



cutoff <- 0.12

mat <- confusionMatrix(real,predito,cutoff)
tp <- mat[2,2]
tn <- mat[1,1]
fp <- mat[2,1]
fn <- mat[1,2]
n <- sum(mat)

# ACC bom pagador (default=0)
100*tn/(tn+fn) # perspectiva modelo (NPV = 1 - FOR) [1-INADIMPLENCIA]
100*tn/(tn+fp) # perspectiva realidade (TNR = 1 - FPR)

# ACC mau pagador (default=1)
100*tp/(tp+fp) # perspectiva modelo (PRECISION = 1 - FDR)
100*tp/(tp+fn) # perspectiva realidade (TPR | RECALL = 1 - FNR)

# ACC GERAL
100*(tn+tp)/n



################################################################################################################
# TESTE DE IMPORTANCIA DAS VARS (verificando o impacto de uma var sozinha. Seria bem interessante ver as vars juntas)

input_manual <- x_test[1,]
colunas <- ncol(x_test)

variavel <- array()
probabilidade <- matrix(nrow=colunas,ncol=2)
for(i in 1:colunas){
  input_manual[1:colunas] <- 0
  for(k in 1:2){
    input_manual[names(input_manual[i])] <- (-1)**k
    variavel[i] <- names(input_manual[i])
    probabilidade[i,k] <- predict_proba(model,matrix(input_manual,nrow=1,ncol=colunas))
  }
}

vars_net <- data_frame(variavel,prob_inf=probabilidade[,1],prob_sup=probabilidade[,2])

vars_net %<>%
  mutate(amp_prob=abs(prob_inf-prob_sup)) %>%
  arrange(desc(amp_prob))

View(vars_net)

 ggplot(vars_net, aes(x=reorder(variavel, -amp_prob), weights=amp_prob)) + theme_minimal() +
   geom_bar() +
   theme(axis.text=element_text(size=9),axis.title=element_text(size=10),axis.text.x=element_text(angle=90),
         legend.position='top',legend.title=element_blank(),legend.text=element_text(size=12)) +
   scale_y_continuous(breaks = seq(from=0,to=1,by=0.1), name='Amplitude de probabilidade')

################################################################################################################

