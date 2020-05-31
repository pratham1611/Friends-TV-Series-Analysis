import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlalchemy
import numpy as np



def connection_handler(data,table):
	user='root'
	pwd='mysql'
	ip='127.0.0.1'
	name='friends'
	conn=sqlalchemy.create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(user,pwd,ip,name))
	data.to_sql(con=conn, name=table, if_exists='append',index = False, chunksize=100000)


def setup(url):
	page = requests.get(url)
	contents = page.content
	soup = BeautifulSoup(contents, 'html.parser')
	return (page,contents,soup)


def load_seasons(url):
	page,contents,soup=setup(url)
	seasons=[]
	for header in soup.find_all('h3'):
		seasons.append(header.text)
	df=pd.DataFrame({'season_name':seasons})
	df.index = np.arange(1, len(df) + 1)
	df['season_id'] = df.index
	connection_handler(df,'seasons')
	
	
def load_episodes(baseurl):

	def extract_season_id(x):
		if len(str(x))>3:
			return str(x[0:2])
		else:
			return str(x[0])

	page,contents,soup=setup(baseurl)
	urls=[]
	episodes=[]
	for collection in soup.find_all('ul'):
		for li in collection.findAll('li'):
			episodes.append(li.text)
		for li in collection.findAll('a'):
			urls.append(baseurl+li['href'])

	df=pd.DataFrame({'episode':episodes,'url':urls})
	df[['episode_id','episode_name']]=df['episode'].str.split(' ',1,expand=True)
	df['episode_id']=np.where((df['episode_id'].str.contains('-')),df['episode_id'].str.split('-').str[0],df['episode_id'])
	df.insert(0, 'season_id',0)
	df['season_id']=df.apply(lambda row: extract_season_id(row['episode_id']),axis=1)
	df['episode_id']=df['episode_id'].str[1:]
	df['episode_id']=pd.to_numeric(df['episode_id'])
	df['season_id']=pd.to_numeric(df['season_id'])
	df=df[['season_id','episode_id','episode_name','url']]
	return urls


def load_dialogues(url):
	page,contents,soup=setup(url)
	season_episode_id=url.split('/')[-1]
	season_id=int(season_episode_id[0:2])
	season_episode_id=season_episode_id.split('.')[0]
	episode_id=season_episode_id[2:]
	if '-' in episode_id:
		episode_id=int(episode_id.split('-')[0])

	else:
		episode_id=int(episode_id)
	dialogues=[]
	for sentence in soup.find_all('p'):
		dialogues.append(sentence.text)
	for sentence in soup.find_all('strong'):
		dialogues.append(sentence.text)

	if len(dialogues)<50: #threshold
		dialogues=soup.get_text().split("\n")

	df = pd.DataFrame({'conversation':dialogues})
	df.insert(0, 'season_id',0)
	df = df.assign(season_id=season_id)
	df.insert(1, 'episode_id',0)
	df = df.assign(episode_id=episode_id)
	df.index = np.arange(1, len(df) + 1)
	df['dialogue_id'] = df.index
	df=df.loc[df['conversation'].str.contains(':')]
	df['characters'] = df['conversation'].str.split(':').str[0]
	df['dialogue'] = df['conversation'].str.split(':').str[1]
	df['dialogue']=df['dialogue'].str.strip()
	df['characters']=df['characters'].str.strip()
	df = df.replace(r'\\n',' ', regex=True)
	df=df[['season_id','episode_id','dialogue_id','dialogue','characters']]
	connection_handler(df,'dialogues')



def main():
	baseurl='https://fangj.github.io/friends/'
	load_seasons(url)
	episode_urls=load_episodes(baseurl)
	#print(episode_urls)
	for url in episode_urls:
		if 'outtakes' in url:
			continue
		load_dialogues(url)









if __name__=='__main__':
	main()