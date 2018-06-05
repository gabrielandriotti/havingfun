library(plyr)
library(dplyr)
library(sparklyr)
library(DBI)
library(ggplot2)


# parametros spark
# nodes: 9 (maquinas slave)
# memory: 30GB (memoria de cada maquina slave)
# cores: 16 (cores de cada maquina slave)
# executors_por_node: 2 (executors por maquina slave)
# --executor-cores: (cores-1)/executors_por_node
# --executor-memory: 0.9*(memory-1)/executors_por_node
# --num-executors: nodes*executors_por_node-1

# configurar a conexao com spark
conf <- spark_config()
conf$spark.executor.cores <- 7
conf$spark.executor.memory <- "13G"
conf$spark.executor.instances <- 17


# conectar spark
sc <- spark_connect(master = "yarn", spark_home = "/usr/lib/spark", config = conf, version = '2.2.0')


# fazer as conexoes com as tabelas originais

tbl_cache(sc, 'default.inativos')
spark_inativos <- tbl(sc, 'default.inativos')

tbl_cache(sc, 'default.clientes')
spark_clientes <- tbl(sc, 'dl_agunico.clientes')


pop_cidades_por_ua <- read.csv(file='/tmp/pop_cidades_por_ua.csv', sep=';',dec='.', header=T,colClasses=c(rep('character',2),rep('numeric',6)))
spark_pop <- copy_to(sc, pop_cidades_por_ua, overwrite = T)



# filtrar somente as colunas importantes

spark_inativos <- spark_inativos %>% 
 select(one_of(c('fconta', 'num_ag','fdenvinat'))) 






# 201201 a 201512

safras_ini <- '201201'
safras_fim <- '201512'


safras <- format(seq(from=as.Date(paste0(safras_ini,'01'),'%Y%m%d'), to=as.Date(paste0(safras_fim,'01'),'%Y%m%d'), by = "month"),'%Y%m')


df_safras <- data_frame()
for (k in safras) {

 #############################################################################################################################
 
 
 # alterar cuidando os formatos
 inicio_safra <- k
 fim_safra <- '201704'
 
 spark_clientes_safra <- spark_clientes %>% 
  filter(ftppess == 1) %>% # para filtrar apenas PF
  filter(date_format(fdataabert, 'yyyyMM') == inicio_safra) %>% 
  select(one_of(c('fconta', 'num_ag','fcpf_cgc','fdnascim','fdataabert','fdinicapit','fposnuc'))) 
 
 # fazer o join entre as tabelas
 
 spark_dataset <- right_join(spark_inativos, spark_clientes_safra, by = c("fconta" = "fconta", "num_ag" = "num_ag"))
 
 dataset_safra <- spark_dataset %>%
  mutate(idade_ao_inativar = (unix_timestamp(fdenvinat,'yyyy-MM-dd')-unix_timestamp(fdnascim,'yyyy-MM-dd'))/60/60/24/365) %>%
  mutate(mes_inativacao = date_format(fdenvinat, 'yyyyMM')) %>%
  mutate(idade_fim_periodo = (unix_timestamp(paste0(fim_safra,'31'),'yyyyMMdd')-unix_timestamp(fdnascim,'yyyy-MM-dd'))/60/60/24/365) %>%
  mutate(idade = ifelse(is.na(fdenvinat)==T,idade_fim_periodo,idade_ao_inativar)) %>%
  mutate(ua = lpad(fposnuc,2,'')) %>%
  filter((mes_inativacao >= inicio_safra & mes_inativacao <= fim_safra) | is.na(mes_inativacao)==T) %>%
  left_join(spark_pop, by = c("ua" = "ua", "num_ag" = "num_ag")) %>%
  filter(is.na(populacao) == F) %>%
  filter(idade > 18) %>%
  sdf_register('spark_safra') %>%
  collect
 
 # dbGetQuery(sc,'select num_ag,count(0) as qtde from spark_safra group by num_ag order by 1')
 
 dataset_safra <- dataset_safra %>%
  mutate(faixa_idade = cut(idade, breaks = c(18, 25, 50, Inf), right = FALSE, labels = c("18-25 anos", "25-50 anos", "50-100 anos"))) %>%
  mutate(faixa_pop = cut(populacao, breaks = c(-Inf, 400000, Inf), right = FALSE, labels = c("0-400k hab", "400k-12M hab"))) %>%
  select(one_of(c('fcpf_cgc','mes_inativacao','faixa_idade','faixa_pop'))) %>%
  group_by(fcpf_cgc) %>% 
  summarize(mes_inativacao = max(mes_inativacao), faixa_idade = first(faixa_idade), faixa_pop = first(faixa_pop))
 
 # conferir numero de cpfs distintos
 # dbGetQuery(sc,'select count(distinct fcpf_cgc) from spark_safra')
 
 
 ativos <- dataset_safra %>%
  group_by(faixa_idade,faixa_pop) %>% 
  summarize(ativos_inicio=n())
 
 
 dataset_safra <- dataset_safra %>%
  filter(is.na(mes_inativacao)==F) %>%
  group_by(mes_inativacao, faixa_idade, faixa_pop) %>%
  summarize(inativos_mes = n())
 
 
 # aqui fazer o lance de colocar pra dentro os meses todos e depois agrupar para ficar certinho
 anos_safra <- format(seq(from=as.Date(paste0(inicio_safra,'01'),'%Y%m%d'), to=as.Date(paste0(fim_safra,'01'),'%Y%m%d'), by = "month"),'%Y%m')
 faixas_idade <- levels(dataset_safra$faixa_idade)
 faixas_pop <- levels(dataset_safra$faixa_pop)
 
 
 
 df_zeros <- data_frame()
 for (i in anos_safra) {
  for (j in faixas_pop) {
   temp <- data_frame(mes_inativacao=rep(i,length(faixas_idade)),faixa_idade=faixas_idade,faixa_pop=rep(j,length(faixas_idade)),inativos_mes=0) 
   df_zeros <- bind_rows(df_zeros,temp)
  }
 }
 
 dataset_safra <- bind_rows(dataset_safra,df_zeros) 
 
 dataset_safra <- dataset_safra %>%
  group_by(mes_inativacao,faixa_idade,faixa_pop) %>%
  summarize(inativos_mes=sum(inativos_mes))
 
 
 # criar var acumulada
 dataset_safra <- dataset_safra %>% 
  group_by(faixa_idade,faixa_pop) %>% 
  mutate(inativos_acum = cumsum(inativos_mes))
 
 
 # trazer os ativos desde o inicio
 dataset_safra <- left_join(dataset_safra, ativos, by = c("faixa_idade" = "faixa_idade","faixa_pop" = "faixa_pop"))
 
 
 # criar var final (% de ativos)
 dataset_safra <- dataset_safra %>%
  mutate(ativos=ativos_inicio-inativos_acum) %>%
  mutate(perc_ativos=ativos/ativos_inicio)
 
 
 # criar num mes para fazer medias depois
 dataset_safra <- dataset_safra %>%
  group_by(faixa_idade,faixa_pop) %>% 
  mutate(num_mes = rank(mes_inativacao, ties.method = "first")) %>%
  ungroup() %>%
  select(one_of(c('num_mes','faixa_idade','faixa_pop','perc_ativos')))


  
 
 df_safras <- bind_rows(df_safras,dataset_safra)
 
 } 
 


# calcular media das safras
df_safras <- df_safras %>%
 mutate(grupo = paste(faixa_idade,faixa_pop,sep=' :: ')) %>%
 group_by(grupo,num_mes) %>%
 summarize(med_perc_ativos=100*mean(perc_ativos))


# para retirar erro do fim do grafico
limite_meses <- max(df_safras$num_mes)-1
df_safras <- df_safras %>%
 filter(num_mes <= limite_meses)

# grafico

 p <- ggplot(df_safras, aes(x=num_mes, y=med_perc_ativos, group=grupo))
 p + geom_line(aes(colour = grupo), linetype = 1) + labs(title=paste0("Safras a partir de: ",safras[1]), x="Meses",y='% Ativos') +
  scale_x_continuous(breaks = seq(0, 60, 12)) +
  theme(axis.line = element_line(size=0.5, colour = "black"),
        panel.grid.major = element_blank(),
        panel.grid.minor = element_blank(),
        panel.border = element_blank(),
        panel.background = element_blank(),
        plot.title=element_text(size = 17),
        text=element_text(size = 14),
        axis.text.x=element_text(colour="black", size = 12),
        axis.text.y=element_text(colour="black", size = 12)) 


 
 
 
 
 
 
# spark_disconnect(sc)
 
 
 
 
 