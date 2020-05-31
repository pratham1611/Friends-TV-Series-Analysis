import mysql.connector
import pandas as pd
import sqlalchemy
import numpy as np
import string
import nltk
from wordcloud import WordCloud
from nltk.corpus import stopwords

stopword = stopwords.words('english')
from textblob import TextBlob
from textblob import Blobber
from textblob.sentiments import NaiveBayesAnalyzer
import re

import json
from ibm_watson import NaturalLanguageUnderstandingV1
from ibm_cloud_sdk_core.authenticators import IAMAuthenticator
from ibm_watson.natural_language_understanding_v1 import Features, EmotionOptions
import pandas as pd
import matplotlib.pyplot as plt

SEASON_SQL='select season_id from seasons'
EPISODE_SQL='select episode_id from episodes where season_id=%d'
DIALOGUE_SQL='select season_id,episode_id,characters,COUNT(*) as dialogue_count '\
'from dialogues where season_id=%d and episode_id=%d group by season_id, episode_id,characters'
SEASON_DIALOGUES_SQL='select season_id, characters,"TOTAL_DIALOGUES" as metric, COUNT(*) as value from dialogues where '\
'(upper(characters) like "%s" or upper(characters) like "%s") and dialogue!="" group by season_id,characters, metric'
SEASON_SENTIMENT_SQL='select * from dialogues where (upper(characters) like "%s" or upper(characters) like "%s") and dialogue!=""'
PERCENT_SHARE_SQL='select a.season_id,a.characters,ROUND(a.value/b.total,3)*100 as value '\
'FROM friends.aggregate_season a join '\
'(select season_id, COUNT(*) as total from dialogues where dialogue!="" group by season_id)b '\
'on a.season_id=b.season_id where a.characters like "%s" and a.metric="TOTAL_DIALOGUES"' 




def remove_punct(text):
	if not str(text).isdigit():
	    removed_punct = ''.join([char for char in text if char not in string.punctuation])
	    return removed_punct

def tokenize(text):
	if text is not None:
			tokenized = re.split('\\s+', text)
			return tokenized

def remove_stopwords(text):
	if text is not None:
    		no_stopwords = [word for word in text if word not in stopword]
    		return no_stopwords


def lemmatize(words,wn):
		stem_words=[wn.lemmatize(t) for t in words]
		return stem_words



def get_data(query,hostname='127.0.0.1',dbname='friends',username='root',pwd='mysql',arraysize=10000):
	db = mysql.connector.connect(host=hostname,database=dbname,user=username,password=pwd)
	cursor = db.cursor(dictionary=True)
	cursor.execute(query)
	results = cursor.fetchmany(arraysize)
	cursor.close()
	db.close()
	return results


def insert_df_to_sql(dataframe,table,hostname='127.0.0.1',dbname='friends',username='root',pwd='mysql'):
	conn=sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(username,pwd,hostname,dbname))
	dataframe.to_sql(con=conn, name=table, if_exists='append',index = False)


def extract_season_total_dialogues(query,character,mapping):
	searchfor=[]
	searchfor.append(character)
	searchfor.append(mapping)
	df=pd.DataFrame(get_data(query),columns=['season_id','characters','metric','value'])
	df.loc[(df['characters'].str.contains('|'.join(searchfor),case=False)), 'characters']=character
	df=df.groupby(['season_id','characters','metric'])[['value']].sum().reset_index()
	insert_df_to_sql(df,'aggregate_season')



def extract_season_sentiment(query,character,mapping):
	searchfor=[]
	searchfor.append(character)
	searchfor.append(mapping)


	def assign_sentiment(row,param):
		if str(row["dialogue"]).isdigit():
			return 'pos',0
		
		obj=param(row["dialogue"])
		return obj.sentiment.classification, max(obj.sentiment.p_pos,obj.sentiment.p_neg)



	def filter_records(words):
		meaningful_words={word for word in words if len(word)>3}
		return list(meaningful_words)


	def get_final_words(words):
		final=""
		for word in words:
			final=final + ' '+word

		return final


	def get_sentiment(words,season_id):
		response = natural_language_understanding.analyze(
			text=words,
			features=Features(emotion=EmotionOptions())).get_result()

		season_emotions[season_id]=response["emotion"]["document"]["emotion"]

	df=pd.DataFrame(get_data(query),columns=['season_id','episode_id','dialogue_id','dialogue','characters'])
	df['nopunct_text']=df['dialogue'].apply(lambda x : remove_punct(x))
	df['tokenized_text'] = df['nopunct_text'].apply(lambda x : tokenize(x))
	df['nostop_text']=df['tokenized_text'].apply(lambda x : remove_stopwords(x))
	df.loc[(df['characters'].str.contains('|'.join(searchfor),case=False)), 'characters']=character
	df=df.groupby(['season_id','characters']).agg({'nostop_text': 'sum'}).reset_index()
	df['meaningful_words']=df['nostop_text'].apply(lambda x : filter_records(x))
	wn = nltk.WordNetLemmatizer()
	df['lemmatized_text']=df['meaningful_words'].apply(lambda x: lemmatize(x,wn))

	df['final_text']=df['lemmatized_text'].apply(lambda x: get_final_words(x))

	authenticator = IAMAuthenticator('SVld380RagsF4KHEVf2vsjIjjg0OZhpbIOLQOQir6zHz')
	natural_language_understanding = NaturalLanguageUnderstandingV1(
		version='2019-07-12',
        authenticator=authenticator)
	natural_language_understanding.set_service_url('https://api.us-east.natural-language-understanding.watson.cloud.ibm.com/instances/98e2efc7-eec3-4ba1-af5f-a9b461065986')
	season_emotions={}
	df.apply(lambda x : get_sentiment(x['final_text'],x['season_id']),axis=1)
	df_emotion = pd.DataFrame(season_emotions)
	df_emotion=df_emotion.transpose().reset_index()
	df_emotion=df_emotion.rename(columns={'index':'season_id'})
	df_emotion.insert(1,'characters',character)
	df_emotion=df_emotion.melt(id_vars=["season_id", "characters"],
		var_name="metric", 
		value_name="value")
	df_emotion['metric']='EMOTION_'+df_emotion['metric'].str.upper()



def extract_percent_share_of_total_dialogues(query):
	df=pd.DataFrame(get_data(query),columns=['season_id','characters','value'])
	df.insert(2,'metric','PER_SHARE_DIALOGUES')
	insert_df_to_sql(df,'aggregate_season')


def dialogue_corpus(dialogue,corpus):
	return corpus+''+dialogue


def extract_common_words(query,character,mapping):
	df=pd.DataFrame(get_data(query),columns=['season_id','episode_id','dialogue_id','dialogue','characters'])
	corpus=''
	common_words={'Chandler','Monica','Rachel','Joey','Ross','Phoebe','Oh','Yeah','Okay','Well','Yes','No', 'God','Hey','something','really'}
	corpus=df['dialogue'].str.cat(sep=' ')
	corpus = remove_punct(corpus)
	tokenized = nltk.word_tokenize(corpus)
	is_noun = lambda pos: pos[:2] == 'NN'
	nouns = [word for (word, pos) in nltk.pos_tag(tokenized) if is_noun(pos)]
	nouns=[word for word in nouns if word not in common_words]
	stopwords = nltk.corpus.stopwords.words('english')
	wordcloud = WordCloud(
        background_color='white',
        stopwords=stopwords,
        max_words=200,
        max_font_size=40, 
        scale=3,
        random_state=1 # chosen at random by flipping a coin; it was heads
    ).generate(str(nouns))
	fig = plt.figure(1, figsize=(12, 12))
	plt.axis('off')
	plt.imshow(wordcloud)
	plt.show()



def main():
	characters = ['Chandler', 'Phoebe','Joey','Ross','Rachel','Monica']
	mapping={
	'Chandler':'CHAN',
	'Phoebe':'PHOE',
	'Joey':'JOEY',
	'Ross':'ROSS',
	'Rachel':'RACH',
	'Monica':'MNCA',
	}
	for character in characters:
		extract_season_total_dialogues(SEASON_DIALOGUES_SQL%('%'+character+'%','%'+mapping[character]+'%'),character,mapping[character])
		extract_season_sentiment(SEASON_SENTIMENT_SQL%('%'+character+'%','%'+mapping[character]+'%'),character,mapping[character])
		extract_percent_share_of_total_dialogues(PERCENT_SHARE_SQL%character)
		extract_common_words(SEASON_SENTIMENT_SQL%('%'+character+'%','%'+mapping[character]+'%'),character,mapping[character])

if __name__=='__main__':
	main()






