# Databricks notebook source
# MAGIC %md
# MAGIC ##Presenting:
# MAGIC ## Name: Pedro Elias Muniz Peres
# MAGIC ## Student ID: 1007023
# MAGIC #ADA-Tech _ Santander Coders
# MAGIC ## Final Project : Statistics I

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading Data:
# MAGIC The data in this dataset were extract from two kaggle sources, listed below:
# MAGIC
# MAGIC - World Energy Consumption: https://www.kaggle.com/datasets/pralabhpoudel/world-energy-consumption
# MAGIC
# MAGIC - Countries of the world: https://www.kaggle.com/datasets/fernandol/countries-of-the-world

# COMMAND ----------

#carregar o data set
!pip install folium
country = spark.read.option("delimiter",";").csv("/FileStore/tables/country-1.csv",header=True,
                      inferSchema = True)
country.createOrReplaceTempView("country")
energy = spark.read.option("delimiter",";").csv("/FileStore/tables/energy-3.csv",header=True,
                      inferSchema = True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Libraries
# MAGIC - Pandas
# MAGIC - Numpy
# MAGIC - Folium
# MAGIC - Plotly
# MAGIC - Seaborn
# MAGIC - Matplotlib
# MAGIC - Itertools
# MAGIC - SciPy
# MAGIC - Math
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initial Treating
# MAGIC
# MAGIC - At first, I chose to work only with renewable energy production data, from 5 different sources. I decided to use the average of the production from 2000 to 2020.
# MAGIC - The data were treated using filtering dataframe concepts and also the spark.sql tool
# MAGIC - The two tables were joined using the spark join method, based on the country code. The country code was manually provided outside databricks, directly in to the dataset, since it was an information missing from the source material.
# MAGIC - The final result was turned into a view.
# MAGIC - At the end, we were left with data from 184 countries.
# MAGIC - The Production of electricity is given in terawatt-hours

# COMMAND ----------

import matplotlib.pyplot as plt



colunas_finais = [
'country_id',
'iso_code',
'ano', 
'biofuel_electricity',
'hydro_electricity',
'nuclear_electricity',
'solar_electricity',
'wind_electricity',
]

energy_colunas_finais = energy[colunas_finais]


energy_maior_2000 = energy_colunas_finais[energy_colunas_finais['ano'] >= 2000]
energy_maior_2000 = energy_maior_2000[energy_maior_2000['country_id'] != 224]
energy_maior_2000 = energy_maior_2000[energy_maior_2000['iso_code'] != 'VIR']
energy_maior_2000.createOrReplaceTempView("energy")

medias_energy = spark.sql( """Select 
                            country_id,
                            iso_code,                      
                            coalesce(AVG(biofuel_electricity),0) as avg_biofuel_electricity,
                            coalesce(AVG(hydro_electricity),0) as avg_hydro_electricity,
                            coalesce(AVG(nuclear_electricity),0) as avg_nuclear_electricity,
                            coalesce(AVG(solar_electricity),0) as avg_solar_electricity,
                            coalesce(AVG(wind_electricity),0) as avg_wind_electricity,
                            coalesce(AVG(biofuel_electricity + hydro_electricity + nuclear_electricity + solar_electricity + wind_electricity),0) as renewable_electricity,
                            if (coalesce(AVG(biofuel_electricity + hydro_electricity + nuclear_electricity + solar_electricity + wind_electricity),0) > 0, 1, 0) as FLG_electricity
                            from energy 
                            group by country_id,iso_code

                            """
)

energia = medias_energy.join(country,medias_energy["country_id"] ==  country["ID"])

energia.createOrReplaceTempView("energia")
display(energia)





# COMMAND ----------

# MAGIC %md
# MAGIC ##Exploring the data:
# MAGIC ### Consider this: in september of 2023, New York hosted the SDG Summit 2023. SDG stands for Sustainable Development Goals.
# MAGIC

# COMMAND ----------

displayHTML('https://www.issup.net/files/styles/large/public/2023-07/Screenshot%202023-07-02%20at%2011.49.28.png?itok=jUE-kKYp')

# COMMAND ----------

# MAGIC %md
# MAGIC #Probabilities
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 1
# MAGIC ### How can we calculate the probability of chosing a random leader and that leader's country is able to produce renewable energy through a certain source?
# MAGIC -Let's assume all 206 countries of this dataset took their place in the summit.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC - We will be creating our events:
# MAGIC
# MAGIC A -> Set1 -> Biofuel \
# MAGIC B -> Set2 -> Hidro \
# MAGIC C -> Set3 -> wind \
# MAGIC D -> Set4 -> solar \
# MAGIC E -> Set5 -> nuclear \
# MAGIC
# MAGIC p_A will stand for propability of event A happening. And so on:
# MAGIC

# COMMAND ----------

import pandas as pd
import numpy as np

paises_totais = spark.sql("Select distinct Country as paises_totais from energia ").toPandas()["paises_totais"]

#paises_geram_biocombustivel
A = spark.sql("Select distinct Country as paises_geram_biocombustivel from energia where avg_biofuel_electricity > 1 ").toPandas()["paises_geram_biocombustivel"]
p_A = len(A)/len(paises_totais)

#paises_geram_hidro
B= spark.sql("Select Country as paises_geram_hidro from energia where avg_hydro_electricity > 1 ").toPandas()["paises_geram_hidro"]
p_B = len(B)/len(paises_totais)

#paises_geram_eolica
C = spark.sql("Select Country as paises_geram_eolica from energia where avg_wind_electricity > 1 ").toPandas()["paises_geram_eolica"]
p_C = len(C)/len(paises_totais)

#paises_geram_solar
D = spark.sql("Select Country as paises_geram_solar from energia where avg_solar_electricity > 1 ").toPandas()["paises_geram_solar"]
p_D = len(D)/len(paises_totais)

#paises_geram_nuclear
E = spark.sql("Select Country as paises_geram_nuclear from energia where avg_nuclear_electricity > 1 ").toPandas()["paises_geram_nuclear"]
p_E = len(E)/len(paises_totais)




# COMMAND ----------

# MAGIC %md
# MAGIC #### Plotting
# MAGIC
# MAGIC #### The chart below shows us the probability of each individual event.
# MAGIC - remember, the event in question is 'picking a random leader at the summit and that leader's country produces renewable energy

# COMMAND ----------

import plotly.express as px

# Sample data
df = pd.DataFrame(dict(
    Fontes = ['biocombustivel','hidro','eolica','solar','nuclear'],
    Probabilidade = [p_A,p_B,p_C,p_D,p_E]))

fig = px.bar(df, x = 'Fontes', y = 'Probabilidade')

fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2.1 (Events Union and Intersection)
# MAGIC ###What is the probability to picking a leader which his country produces solar energy OR (hidro AND solar energy)
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC With: 
# MAGIC
# MAGIC A -> Set1 -> BioFuel\
# MAGIC B -> Set2 -> Hidro\
# MAGIC C -> Set3 -> Wind\
# MAGIC D -> Set4 -> solar\
# MAGIC E -> Set5 -> nuclear
# MAGIC
# MAGIC Than:\
# MAGIC $$P(C U (B int D))$$ \
# MAGIC $$P(C) + P(B int D) - P(C int P(B int D))$$ \
# MAGIC $$P(C) + (P(B int D) - (P(C)*P(B int D)))$$
# MAGIC  

# COMMAND ----------

# MAGIC %md
# MAGIC We need the values for the intersections. We can do this for every intersection combination by using the following code:

# COMMAND ----------

#combinações
from itertools import combinations
sets = [A,B,C,D,E] #1,2,3,4,5
combinacoes = {}

for comboSize in range(2,len(sets)+1):
    for combo in combinations(range(len(sets)),comboSize):
        intersection = sets[combo[0]]
        for i in combo[1:]:
            nova_interseccao = pd.Series(list(set(intersection) & set(sets[i])))
            combinacoes[" and ".join(f"Set{i+1}" for i in combo)] = nova_interseccao


for i in combinacoes:
    print(f"{i} : {len(combinacoes[i])/len(paises_totais)}")



# COMMAND ----------

p_B_int_D = len(combinacoes['Set2 and Set4'])/len(paises_totais)
Resposta_1 = p_C + (p_B_int_D) - (p_C * (p_B_int_D))
print(f"The probability is {Resposta_1:.2f}")





# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2.2 (Conditional Probability)
# MAGIC ### If the picked leader produces Hidro eletricity, what is the propability he also produces nuclear?
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC With: 
# MAGIC
# MAGIC A -> Set1 -> BioFuel\
# MAGIC B -> Set2 -> Hidro\
# MAGIC C -> Set3 -> Wind\
# MAGIC D -> Set4 -> solar\
# MAGIC E -> Set5 -> nuclear
# MAGIC
# MAGIC Than:\
# MAGIC $$P(E|B)$$\
# MAGIC $$ P(EintB) / P(B)$$

# COMMAND ----------

p_E_int_B = len(combinacoes['Set2 and Set5'])/len(paises_totais)
Resposta_2 = p_E_int_B / p_B
print(f"The probability is {Resposta_2:.2f}")


# COMMAND ----------

# MAGIC %md
# MAGIC ##Question 2.3 (Distribution)
# MAGIC
# MAGIC ### Let's suppose that in one of the meetings of the summit, 10 leaders will be picked at random and sat together in a meeting:
# MAGIC ### what is the probability that 7 of the 10 leaders produces nuclear electricity
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - in this case we might apply a Binomial distribution
# MAGIC - We know the probability of picking a leader that produces nuclear energy = p_E
# MAGIC - We have 10 picks and need 7 successes 
# MAGIC

# COMMAND ----------

from scipy.stats import binom

dist = binom(n=10, p=p_E)

print(f'The probability that 7 of the 10 leaders picked produces nuclear energy is: {(dist.pmf(7))*100:.4f} %') 


# COMMAND ----------

# MAGIC %md
# MAGIC # Correlation
# MAGIC ##Renewable x GDP
# MAGIC #### Take a look at all the countries that has a average production of electricity through renewable sources greater than 0
# MAGIC

# COMMAND ----------


import folium
countries_url = ("http://geojson.xyz/naturalearth-3.3.0/ne_50m_admin_0_countries.geojson")
m = folium.Map(location=(30,10), zoom_start=3)
folium.Choropleth(
    geo_data = countries_url,
    data = energia.toPandas(),
    columns = ["iso_code","FLG_electricity"],
    key_on = "feature.properties.su_a3",
    #fill_color="RdYlGn_r",
    fill_opacity=0.8,
    line_opacity=0.3,
    bins = 36,
    nan_fill_color="white",
    ).add_to(m)
display(m)

# COMMAND ----------

# MAGIC %md
# MAGIC -- The chart above makes it seem as if most of the countries of the world has an impact in the production of renewable energy
# MAGIC
# MAGIC -- However, if we look at the contribution from each country, we have a surprise:

# COMMAND ----------

import folium
countries_url = ("http://geojson.xyz/naturalearth-3.3.0/ne_50m_admin_0_countries.geojson")
m = folium.Map(location=(30,10), zoom_start=3)
folium.Choropleth(
    geo_data = countries_url,
    data = energia.toPandas(),
    columns = ["iso_code","renewable_electricity"],
    key_on = "feature.properties.su_a3",
    #fill_color="RdYlGn_r",
    fill_opacity=0.8,
    line_opacity=0.3,
    bins = 36,
    nan_fill_color="white",
    ).add_to(m)
display(m)

# COMMAND ----------

# MAGIC %md 
# MAGIC - looking at the map above, we see that only a handfull of countries have a significant impact in the electricity production through renewable sources.
# MAGIC
# MAGIC - This raises the questions. What influences the renewable energy production? At first, let's take a look at the gdp:
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###Question 3 (Pearson)
# MAGIC
# MAGIC #### Is there any correlation between the per capita GDP and the production of electricity through renewable energy?

# COMMAND ----------

#Existe correlação entre o PIB/per capita e o uso de energias renováveis? Pearson Correlation
import seaborn as sns
import scipy as sp
sns.set(rc={'figure.figsize':(15,10)})
fig, axs = plt.subplots(3, 2)
fig.tight_layout()
r1, p1 = sp.stats.pearsonr(x=(energia.toPandas()['GDP ($ per capita)']), y=energia.toPandas()['avg_hydro_electricity'])

sns.scatterplot(data=energia.toPandas(), x="GDP ($ per capita)", y="avg_hydro_electricity", ax = axs[0,0]).set_title(f"Hidroelétrica \n R = {r1}")

r2, p2 = sp.stats.pearsonr(x=energia.toPandas()['GDP ($ per capita)'], y=energia.toPandas()['avg_wind_electricity'])
sns.scatterplot(data=energia.toPandas(), x="GDP ($ per capita)", y="avg_wind_electricity", ax = axs[0,1]).set_title(f"Eólica \n R = {r2}")

r3, p3 = sp.stats.pearsonr(x=energia.toPandas()['GDP ($ per capita)'], y=energia.toPandas()['avg_solar_electricity'])
sns.scatterplot(data=energia.toPandas(), x="GDP ($ per capita)", y="avg_solar_electricity", ax = axs[1,0]).set_title(f"Solar \n R = {r3}")

r4, p4 = sp.stats.pearsonr(x=energia.toPandas()['GDP ($ per capita)'], y=energia.toPandas()['avg_nuclear_electricity'])
sns.scatterplot(data=energia.toPandas(), x="GDP ($ per capita)", y="avg_nuclear_electricity", ax = axs[1,1]).set_title(f"Nuclear \n R = {r4}")

r5, p5 = sp.stats.pearsonr(x=energia.toPandas()['GDP ($ per capita)'], y=energia.toPandas()['avg_biofuel_electricity'])
sns.scatterplot(data=energia.toPandas(), x="GDP ($ per capita)", y="avg_biofuel_electricity", ax = axs[2,0]).set_title(f"BioCombustível \n R = {r5}")


fig.subplots_adjust(hspace=0.5, wspace=0.5)



# COMMAND ----------

# MAGIC %md
# MAGIC - The pearson correlation suggests a low correlation among renewable energy and gdp. However, we might need to explore it even further
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #Hypothesis Test
# MAGIC
# MAGIC ### We can use some hypothesis tests to verify the result presented at the correlation

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 4 (Assessing the Hypothesis)
# MAGIC ### We want to see if the average production of the top20 gdp countries is higher than the rest of the world. For that we need two hypothesis:
# MAGIC #### H0 - The average renewable energy production of the top 20 gdp per capita countries is the same as the rest of the world.
# MAGIC ##### We wanna accept or refute this hypothesis. After that we can check on H1
# MAGIC #### H1 - The average renewable energy production of the top 20 gdp per capita countries is greater than the rest of the world.

# COMMAND ----------

#media da geração de energia limpa dos 20 países de maior pib é maior do que a dos demais países?
from scipy.stats import ttest_ind
#H0
#H1
GDP_paises = spark.sql("""Select `GDP ($ per capita)` as gdp FROM energia order by `GDP ($ per capita)` desc""").toPandas()["gdp"]
 
threshold_GDP = GDP_paises[19]  
querry_maior = """Select renewable_electricity as geracao FROM energia where `GDP ($ per capita)` >= {}""".format(threshold_GDP)

querry_menor = """Select renewable_electricity as geracao FROM energia where `GDP ($ per capita)` < {}""".format(threshold_GDP)

geracao_10_paises = spark.sql(querry_maior).toPandas()["geracao"]
geracao_demais_paises = spark.sql(querry_menor).toPandas()["geracao"]

p_value = ttest_ind(geracao_10_paises,geracao_demais_paises,equal_var = False)[1]

if p_value <= 0.05:
    print(f'H0 is refuted with p-value:{p_value}')
else:
    print(f'H0 cannot be refuted. The p-value is:{p_value}')







# COMMAND ----------

# MAGIC %md
# MAGIC ##Question 5 (Refuting the Hypothesis?)

# COMMAND ----------

# MAGIC %md
# MAGIC -Still, if we check on the relationship between the average production of the top20 countries (by gdp) and the rest of the world, we will see that the top20 has a much higher average:

# COMMAND ----------

media_maior = """Select avg(renewable_electricity) as media 
                            FROM energia where `GDP ($ per capita)` >= {}""".format(threshold_GDP)

media_menor = """Select avg(renewable_electricity) as media 
                            FROM energia where `GDP ($ per capita)` < {}""".format(threshold_GDP)

media_10_paises = spark.sql(media_maior).toPandas()["media"]
media_demais_paises = spark.sql(media_menor).toPandas()["media"]

print(media_10_paises/media_demais_paises)

# COMMAND ----------

# MAGIC %md
# MAGIC - So why can't we still refute the hypothesis H0?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 6 (Explanation)

# COMMAND ----------

display(energia.filter("`GDP ($ per capita)` >= 27600"))

# COMMAND ----------

# MAGIC %md
# MAGIC - The boxplot below shows us that the data for the top20 gdp countries has many outliers. Theses outliers move the average up, inducing a thought that indeed the top20 countries should have a higher production. However, some of the top20 countries has no production at all. That is why we can't refute h0.

# COMMAND ----------

import plotly.express as px
df = energia.filter("`GDP ($ per capita)` >= 27600").toPandas()
fig = px.box(df, y="renewable_electricity")
fig.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #Sampling

# COMMAND ----------

# MAGIC %md
# MAGIC ##Question 7(Sample size)

# COMMAND ----------

# MAGIC %md
# MAGIC - When talking about sampling, we need to first understand what is the proper size of the sample, taking in to consideration the confidence interval and the expected error.
# MAGIC
# MAGIC - When sampling finite populations, we must consider the folling formula:

# COMMAND ----------

# MAGIC %md
# MAGIC $$ n= (N * (Z * \sigma / E)^2) / ((N-1) * E^2 + (Z * \sigma )^2) $$

# COMMAND ----------

import math
#95% confidence interval
Z = 1.96 
N = len(paises_totais)
sigma = energia.select("renewable_electricity").rdd.flatMap(lambda x: x).stdev()

E = 2

n=math.ceil((N * (Z*sigma/E)**2)/((N-1)*(E**2)+(Z*sigma)**2))
print(f"standard deviation: {sigma}")
print(f"Sample size: {n}")

# COMMAND ----------

# MAGIC %md
# MAGIC - The result above shows us that a sample size of 52 countries picked at random should give us similar results at 95,5% confidence interval.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Question 8 (Sampling)
# MAGIC ### We need to come up with a way to sample our data, now data we know the ideal sampling size. 
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC - Ideally we could try the random sampling method, but for that we need to know the proability of a country being picked.
# MAGIC -Luckilly we can have that information:
# MAGIC -- if we have 206 countries in our data, and our sampling size is 52, that means that our probability of being picked(p_p) is:
# MAGIC $$ p_p = 52 / 206 = 25.24 \\% $$

# COMMAND ----------

p_p = n/len(paises_totais)
sample_data = energia.sample(False, p_p)
display(sample_data)
sample_data.createOrReplaceTempView("sample_data")

# COMMAND ----------

# MAGIC %md
# MAGIC ##Question 9 (Testing Our Sample)
# MAGIC  - now lets use our sample to plot the probabilites we saw at question 1 for the full population

# COMMAND ----------

paises_totais_p = spark.sql("Select distinct Country as paises_totais from sample_data ").toPandas()["paises_totais"]

#paises_geram_biocombustivel
A_p = spark.sql("Select distinct Country as paises_geram_biocombustivel from sample_data where avg_biofuel_electricity > 1 ").toPandas()["paises_geram_biocombustivel"]
p_A_p = len(A_p)/len(paises_totais_p)

#paises_geram_hidro
B_p= spark.sql("Select Country as paises_geram_hidro from sample_data where avg_hydro_electricity > 1 ").toPandas()["paises_geram_hidro"]
p_B_p = len(B_p)/len(paises_totais_p)

#paises_geram_eolica
C_p = spark.sql("Select Country as paises_geram_eolica from sample_data where avg_wind_electricity > 1 ").toPandas()["paises_geram_eolica"]
p_C_p = len(C_p)/len(paises_totais_p)

#paises_geram_solar
D_p = spark.sql("Select Country as paises_geram_solar from sample_data where avg_solar_electricity > 1 ").toPandas()["paises_geram_solar"]
p_D_p = len(D_p)/len(paises_totais_p)

#paises_geram_nuclear
E_p = spark.sql("Select Country as paises_geram_nuclear from sample_data where avg_nuclear_electricity > 1 ").toPandas()["paises_geram_nuclear"]
p_E_p = len(E_p)/len(paises_totais_p)



# COMMAND ----------

import pandas as pd
from plotly.subplots import make_subplots
import plotly.graph_objs as go

fig = make_subplots(rows=1, cols=2,
                    subplot_titles=["Sample",
                                    "Population"]
                   )
fig.add_trace(go.Bar(x=['biofuel','hidro','eolica','solar','nuclear'],
                     y=[p_A_p,p_B_p,p_C_p,p_D_p,p_E_p],
                     name="Sample"),
             row=1, col=1)
fig.add_trace(go.Bar(x=['biofuel','hidro','eolica','solar','nuclear'],
                     y=[p_A,p_B,p_C,p_D,p_E],
                     name="Population"),
             row=1, col=2)
fig['layout']['xaxis1'].update(dict(
        tickmode = 'array',
        tickvals = ['biofuel','hidro','eolica','solar','nuclear'],
        ticktext = ['biofuel','hidro','eolica','solar','nuclear']))

fig['layout']['xaxis2'].update(dict(
        tickmode = 'array',
        tickvals = ['biofuel','hidro','eolica','solar','nuclear'],
        ticktext = ['biofuel','hidro','eolica','solar','nuclear']))

fig.show()

# COMMAND ----------


