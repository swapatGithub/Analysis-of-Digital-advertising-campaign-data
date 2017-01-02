#function to count fold positions	
def cntFoldPos(values):
    cnt0=0
    cnt1=0
    cnt2=0
    for each in values:
        if each==0:
            cnt0=cnt0+1
        elif each==1:
            cnt1=cnt1+1
        elif each==2:
            cnt2=cnt2+1
    return 0 if cnt0==max(cnt0,cnt1,cnt2) else 1 if cnt1==max(cnt0,cnt1,cnt2) else 2	
	
#function to identify viewed and converted and also those which are convereted but not in viewed dataset
def viewedAndPV_converted(impr_data_pv_conv, impr_data_imp_noClicks):
	list_pv_conv_viewed=[]
	users_notViewedAds_converted=[]

	for inde, seri in impr_data_pv_conv.iterrows():
		if seri[15] in impr_data_imp_noClicks['user_id_64'].values:
			for inde1, seri1 in impr_data_imp_noClicks[impr_data_imp_noClicks["user_id_64"]==seri[15]].iterrows():
				if seri[3]==seri1[3] and seri[2]==seri1[2] and seri[30]==seri1[30] and seri[29]==seri1[29]:           
					if inde1 not in list_pv_conv_viewed:
						list_pv_conv_viewed.append(inde1)
		else:
			if seri[15] not in users_notViewedAds_converted:
				users_notViewedAds_converted.append(seri[15])
				
	res_tuple=(list_pv_conv_viewed,users_notViewedAds_converted)	
	return res_tuple

#function to identify Ads which are viewed and clicked 
def viewedAndClicked(impr_data_click):
	events_viewed_and_clicked = []
	for ind, val in impr_data_click["user_id_64"].iteritems():		
		if val in impr_data_imp["user_id_64"].values: 
			height = impr_data_click["height"].get(ind)
			width = impr_data_click["width"].get(ind)
			C_id = impr_data_click["creative_id"].get(ind)
			camp_id = impr_data_click["campaign_id"].get(ind)
			for inde, seri in impr_data_imp[impr_data_imp["user_id_64"]==val].iterrows():            
				if height==seri[3] and width==seri[2] and C_id==seri[30] and camp_id==seri[29]:
					if inde not in events_viewed_and_clicked:
						events_viewed_and_clicked.append(inde) 
	return events_viewed_and_clicked

def main():
    impr_data = pd.read_csv("C:/Users/spawar5/Downloads/query_result/query_result.csv")
	
	#Segregating the data into 4 different event types
	impr_data_imp = impr_data[impr_data["event_type"]=="imp"]
	impr_data_click = impr_data[impr_data["event_type"]=="click"]
	impr_data_pc_conv = impr_data[impr_data["event_type"]=="pc_conv"]
	impr_data_pv_conv = impr_data[impr_data["event_type"]=="pv_conv"]
	
	#Now, next line will check ads which are viewed and clicked
	events_viewed_and_clicked = viewedAndClicked(impr_data_click) 	
	#Number of events in impression which are clicked at some point in time by same user - 1765
	
	#Deleting these events from impression data as we need to analyse view data only
	impr_data_imp_noClicks = impr_data_imp.drop(events_viewed_and_clicked) #dataframe has events which are viewed only and not clicked, count of rows - 1512780
	impr_data_imp_withClicks = impr_data_imp.loc[events_viewed_and_clicked] #dataframe has events which are viewed and clicked
	
	#to get the records which are viewed and PV_converted and also those records which are converted without having viewed
	tuple_viewed_PVConverted = viewedAndPV_converted(impr_data_pv_conv, impr_data_imp_noClicks)

	list_pv_conv_viewed = tuple_viewed_PVConverted[0]
	users_notViewedAds_converted = tuple_viewed_PVConverted[1]
	
	#Dataframe having ads which are viewed and converted
	df_viewed_PV_conv = impr_data_imp_noClicks.loc[list_pv_conv_viewed]
	#Dataframe having ads which are viewed but not converted
	df_viewed_NOT_PVconv = impr_data_imp_noClicks.drop(list_pv_conv_viewed)
	
	print('num of users viewed and converted - ',len(df_viewed_PV_conv.user_id_64.unique()), 'And number of users in converted only - ',len(users_notViewedAds_converted))
	#Number of users in viewed and converted are 504 and number of users which are there in converted dataset only are 1128. This shows that digital campaign has not been successful #enough and people who already knew about the company have high chance of converting
	
	####################Data mining approach to see which impression related factors have impact on customer conversion#######################
	#Changing the granularity of records for usign data mining technique effectively. In new datasets a unique row is identified by userID, campaign ID and creating ID
	user_ad_group_converted = df_viewed_PV_conv.groupby(['user_id_64','campaign_id','creative_id']) #num of user-Ad combination viewed and converted -  504
	user_ad_group_notConverted = df_viewed_NOT_PVconv.groupby(['user_id_64','campaign_id','creative_id']) #num of user-Ad combination viewed and NOT converted -  917870
	
	#Creating initial columns for two dataframes
	uIDs = []
	camp_ids = []
	creat_ids = []
	for each in list(user_ad_group_converted.indices.keys()):
		uIDs.append(each[0])
		camp_ids.append(each[1])
		creat_ids.append(each[2])

	uIDs1 = []
	camp_ids1 = []
	creat_ids1 = []
	for each in list(user_ad_group_notConverted.indices.keys()):
		uIDs1.append(each[0])
		camp_ids1.append(each[1])
		creat_ids1.append(each[2])

	#New dataframes having unique row based on user, campaign and creative_ad
	df_grouped_User_Ad = pd.DataFrame(data={'user_id_64':uIDs,'campaign_id':camp_ids,'creative_id':creat_ids})
	df_grouped_UserAd_notConverted = pd.DataFrame(data={'user_id_64':uIDs1,'campaign_id':camp_ids1,'creative_id':creat_ids1})		
	
	#Creating a new columns by aggregating over existing columns based on groups of campaign, creative_ad and user and adding to the dataframes (feature engineering)
	df_grouped_User_Ad['width'] = list(user_ad_group_converted.width.mean())
	df_grouped_User_Ad['height'] = list(user_ad_group_converted.height.mean())
	df_grouped_User_Ad['count_ad_viewed'] = list(user_ad_group_converted.datetime.count())
	df_grouped_User_Ad['max_foldPosition'] = list(user_ad_group_converted.fold_position.apply(cntFoldPos))
	df_grouped_User_Ad['avg_media_CPM'] = list(user_ad_group_converted.media_cost_dollars_cpm.mean())
	df_grouped_User_Ad['avg_eap'] = list(user_ad_group_converted.eap.mean())
	df_grouped_User_Ad['avg_creative_freq'] = list(user_ad_group_converted.creative_freq.mean())
	df_grouped_User_Ad['time_ad_shown'] = list(user_ad_group_converted.creative_rec.max()- user_ad_group_converted.creative_rec.min())

	df_grouped_UserAd_notConverted['width'] = list(user_ad_group_notConverted.width.mean())
	df_grouped_UserAd_notConverted['height'] = list(user_ad_group_notConverted.height.mean())
	df_grouped_UserAd_notConverted['count_ad_viewed'] = list(user_ad_group_notConverted.datetime.count())
	df_grouped_UserAd_notConverted['max_foldPosition'] = list(user_ad_group_notConverted.fold_position.apply(cntFoldPos))
	df_grouped_UserAd_notConverted['avg_media_CPM'] = list(user_ad_group_notConverted.media_cost_dollars_cpm.mean())
	df_grouped_UserAd_notConverted['avg_eap'] = list(user_ad_group_notConverted.eap.mean())
	df_grouped_UserAd_notConverted['avg_creative_freq'] = list(user_ad_group_notConverted.creative_freq.mean())
	df_grouped_UserAd_notConverted['time_ad_shown'] = list(user_ad_group_notConverted.creative_rec.max()- user_ad_group_notConverted.creative_rec.min())
	
	#creating a label variable
	df_grouped_User_Ad['converted'] = 1
	df_grouped_UserAd_notConverted['converted'] = 0	
	
	df_final = df_grouped_User_Ad.append(df_grouped_UserAd_notConverted)
	
	#Shuffling the dataframe rows
	df_final = df_final.iloc[np.random.permutation(len(df_final))]
	df_final = df_final.reset_index(drop=True)
	
	#Applying SMOTEEN algorithm for oversampling and undersampling
	sm = SMOTEENN()
	X_resampled, y_resampled = sm.fit_sample(df_final[df_final.columns[:-1]], df_final['converted'])

	#print('Positive responses - ',y_resampled.nonzero()[0].size) -> 314908
	#print('Negative responses - ',y_resampled.size-y_resampled.nonzero()[0].size) -> 917870

	###########Implementing a logistic model####################
	#Removing first three columns as they are more like a combined ID field for each row
	features_one = [each[3:] for each in X_resampled]
	target = y_resampled
	#70-30 training-test data split
	Xtrain, Xtest, Ytrain, Ytest = train_test_split(features_one,target,random_state=1)
	#Building a model
	logistic = linear_model.LogisticRegression()
	my_logit_one = logistic.fit(Xtrain, Ytrain)

	#model scoring with test data
	print(my_logit_one.score(Xtest,Ytest))

if __name__ == '__main__':
    main()