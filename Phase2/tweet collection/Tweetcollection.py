from __future__ import absolute_import, print_function


from tweepy.streaming import StreamListener

from tweepy import OAuthHandler

from tweepy import Stream


import time



consumer_key="5rkFkJDFPVBVDSvtNtoVpFpgq"

consumer_secret="VkK5YIETgPFNgKwt1tFHRSrztnNW00cbVaxCUAgMYqGrI5VHxa"

access_token="4843507186-Kc982XGeypByuevVKdAhbMmEwnOmWWxgw247hUV"

access_token_secret="bKFAS5HDMzRMuckOnbMIjwjDxnKzUeivIz5VbENrs4soR"



class TweitListener(StreamListener):
   
    
	def on_data(self, tweitinfo):
        
		try:
            
			
            
			tweitInfoJsonTeam9 = open('tweitColtoday.txt','a')
            
			tweitInfoJsonTeam9.write(tweitinfo)
            
			tweitInfoJsonTeam9.write('\n')
            
			tweitInfoJsonTeam9.close()
            
			return True
        
		except BaseException as e:
            
			print ('Problem collecting tweit information in JSON,',str(e))
            
			time.sleep(5)

    

	def on_error(self, error):
        
		print(error)



if __name__ == '__main__':
    
	twt = TweitListener()
    
	auth = OAuthHandler(consumer_key, consumer_secret)
    
	auth.set_access_token(access_token, access_token_secret)

    
	
	stream = Stream(auth, twt)
    
	stream.filter(track=["#JebBush","#RealBenCarson","#ChrisChristie","#tedcruz","#CarlyFiorina","#gov_gilmore","#JohnKasich","#marcorubio","#realDonaldTrump","#HillaryClinton","#BernieSanders"])
