import datetime
import string
import pandas as pd
from sentence_transformers import SentenceTransformer, util
from database import MssqlHandler
from tools.gchat_logging import send_to_g_chat
from sheet_link import create_editable_sheet

model = SentenceTransformer('all-MiniLM-L6-v2')
READ_SQL_CXN = MssqlHandler("r")


class DailyAccuracy:
    def __init__(self):
        self.current_date = datetime.date.today()
        self.previous_day = self.current_date - datetime.timedelta(days=1)
        self.three_days_ago = self.current_date - datetime.timedelta(days=3)
        self.formatted_date = self.previous_day.strftime("%dth %b'%y")

    def operations(self, df):
        # Drop duplicates
        df = df.drop_duplicates()
        # Drop NA from AgentReply and ResponseGenie
        df = df.dropna(subset=['AgentReply'])
        df = df.dropna(subset=['ResponseGenie'])

        # Check if there is a mode for 'AgentReply'
        if not df['AgentReply'].empty:
            common_ssre_reply = df['AgentReply'].mode()[0]
            # Remove rows containing the common "ssre reply"
            df = df[df['AgentReply'] != common_ssre_reply]

        # Drop NA from AgentReply and ResponseGenie
        df = df.dropna(subset=['AgentReply'])
        df = df.dropna(subset=['ResponseGenie'])

        # Remove Twitter handles and hashtags
        df['ResponseGenie'] = df['ResponseGenie'].str.replace(r'@\w+', '', regex=True)
        df['AgentReply'] = df['AgentReply'].str.replace(r'@\w+', '', regex=True)
        df['ResponseGenie'] = df['ResponseGenie'].str.replace(r'#\w+', '', regex=True)
        df['AgentReply'] = df['AgentReply'].str.replace(r'#\w+', '', regex=True)

        # Remove punctuation
        df['ResponseGenie'] = df['ResponseGenie'].str.replace(f'[{string.punctuation}]', '', regex=True)
        df['AgentReply'] = df['AgentReply'].str.replace(f'[{string.punctuation}]', '', regex=True)

        return df

    def calculate_sentence_similarity(self, sentence1, sentence2):
        # Encode the sentences into embeddings
        embedding1 = model.encode(sentence1, convert_to_tensor=True)
        embedding2 = model.encode(sentence2, convert_to_tensor=True)
        # Calculate cosine similarity
        cosine_similarity = util.pytorch_cos_sim(embedding1, embedding2)
        return cosine_similarity.item()

    def brand_genie_data(self):
        READ_SQL_CXN.execute(""" SELECT 
        mc.categoryname,
        mb.BrandName,
        mb.BrandID
        FROM mstBrands_metadata bm
        INNER JOIN mstbrands mb ON bm.brandid = mb.brandid
        INNER JOIN mstcategories mc ON mb.categoryid = mc.id
        WHERE IsSuggestedResponseEnabled = 1
        AND ISNULL(isbrandactive, 0) = 1
        AND ISNULL(iscategoryactive, 0) = 1 """)
        df = READ_SQL_CXN.fetch_df()
        return df

    def get_tag_info(self, tag_id, category_name):
        try:
            READ_SQL_CXN.execute(f"""
               SELECT ExistingTagID, channeltype, Brandid, Categoryid, authorid, Channelgroupid
               FROM {category_name}.dbo.tag_{category_name} WITH (NOLOCK)
               WHERE tagid = {tag_id};
               """)
            df_ = READ_SQL_CXN.fetch_df()
            if not df_.empty:
                return df_.iloc[0]
        except:
            pass
        return [None, None, None, None, None, None]

    def data(self, category_name, brand_name, brandid):
        READ_SQL_CXN.execute(f"""
                SELECT DATEADD(MINUTE, 330, InsertedDate) AS date,Response_Text as ResponseGenie, *
                FROM {category_name}.dbo.mstAISuggestedTokenDetails with (NOlock)
                WHERE CONVERT(DATE, DATEADD(MINUTE, 330, InsertedDate)) = '{self.previous_day}'
                and Brandid={brandid}
                ORDER BY date DESC;
                """
                             )

        df = READ_SQL_CXN.fetch_df()

        tag_info_columns = ["ExistingTagID", "channeltype", "Brandid", "Categoryid", "authorid", "Channelgroupid"]
        df[tag_info_columns] = df["TagID"].apply(lambda x: pd.Series(self.get_tag_info(x, category_name)))

        df["AgentReply"] = ""

        for i in df[(df["ai_feature_type"] == 2)].index:
            try:
                READ_SQL_CXN.execute(f"""EXEC LB3_GetUserCommunicationHistory_V51
                        @CategoryName = '{category_name}',
                        @BrandName = '{brand_name}',
                        @EndDate = '{self.current_date} 18:29:59',
                        @AuthorID = '{df["authorid"][i]}',
                        @ChannelGroupID = {int(df["Channelgroupid"][i])},
                        @TicketID = {int(df["ExistingTagID"][i])},
                        @From = 1,
                        @To = 50,
                        @IsActionableData = 0,
                        @CurrentUserID = NULL;""")
                # print(ticket_query)

                df_ = READ_SQL_CXN.fetch_df()
                df_sorted = df_.sort_values(by='CreatedDate', ascending=False)
                df_sorted = df_sorted.reset_index(drop=True)
                # Added assignment to the line below
                index = df_sorted[df_sorted["TagID"] == df["TagID"][i]].index[0] - 1
                df.loc[i, "AgentReply"] = df_sorted["Description"][index]
            except:
                df.loc[i, "AgentReply"] = None

        for i in df[(df["ai_feature_type"] == 0)].index:
            try:
                READ_SQL_CXN.execute(f"""EXEC LB3_GetUserCommunicationHistory_V51
                        @CategoryName = '{category_name}',
                        @BrandName = '{brand_name}',
                        @EndDate = '{self.current_date} 18:29:59',
                        @AuthorID = '{df["authorid"][i]}',
                        @ChannelGroupID = {int(df["Channelgroupid"][i])},
                        @TicketID = {int(df["ExistingTagID"][i])},
                        @From = 1,
                        @To = 50,
                        @IsActionableData = 0,
                        @CurrentUserID = NULL;""")
                # print(ticket_query)

                df_ = READ_SQL_CXN.fetch_df()
                df_sorted = df_.sort_values(by='CreatedDate', ascending=False)
                df_sorted = df_sorted.reset_index(drop=True)
                # Added assignment to the line below
                index = df_sorted[df_sorted["TagID"] == df["TagID"][i]].index[0]
                df.loc[i, "AgentReply"] = df_sorted["Description"][index]
            except:
                df.loc[i, "AgentReply"] = None
        return df


    def alert_formatting(self):
        try:
            brand_df = self.brand_genie_data()
            # data1 = {'categoryname': ['LocobuzzSupportDB'], 'BrandName': ['ORMXCares']}
            # brand_df = pd.DataFrame(data1)
            total_response, kb, success_kb, out_of_kb, success_out_of_kb = [], [], [], [], []
            kb_success_per, out_of_kb_success_per = [],[]
            for index, row in brand_df.iterrows():
                try:
                    res1 = self.data(row['categoryname'], row['BrandName'], row['BrandID'])
                    res = self.operations(res1)

                    responses = len(res)
                    total_response.append(responses)

                    if responses > 0:
                        true_caution = res['caution'].sum()
                        false_caution = responses - true_caution
                        out_of_kb.append(true_caution)
                        kb.append(false_caution)
                        kb_similarity_score, out_of_kb_similarity_score = [], []

                        for i, j, k in zip(res["ResponseGenie"].values, res["AgentReply"].values,
                                           res["caution"].values):
                            similarity_score = self.calculate_sentence_similarity(str(i), str(j))
                            if not k:
                                kb_similarity_score.append(similarity_score)
                            else:
                                out_of_kb_similarity_score.append(similarity_score)

                        similarity_threshold = 0.8

                        kb_count_similarity_score = sum(
                            1 for score in kb_similarity_score if score >= similarity_threshold)
                        out_of_kb_count_similarity_score = sum(
                            1 for score in out_of_kb_similarity_score if score >= similarity_threshold)

                        success_kb.append(kb_count_similarity_score)
                        success_out_of_kb.append(out_of_kb_count_similarity_score)

                        # Check for division by zero and handle it gracefully
                        kb_success_per.append(
                            f'{(kb_count_similarity_score / false_caution) * 100:.2f}' if false_caution != 0 else 'N/A')
                        out_of_kb_success_per.append(
                            f'{(out_of_kb_count_similarity_score / true_caution) * 100:.2f}' if true_caution != 0 else 'N/A')

                    else:
                        kb.append(None)
                        success_kb.append(None)
                        out_of_kb.append(None)
                        success_out_of_kb.append(None)
                        kb_success_per.append(None)
                        out_of_kb_success_per.append(None)

                except Exception as e:
                    print(f"An error occurred for Category: {row['categoryname']}, BrandID: {row['BrandID']}")
                    print(f"Error: {str(e)}")
                    continue

            brand_df["Total Response"] = total_response
            brand_df["KB "] = kb
            brand_df["Success of KB"] = success_kb
            brand_df["% Success of KB"] = kb_success_per
            brand_df["Out of KB "] = out_of_kb
            brand_df["Success of Out of KB"] = success_out_of_kb
            brand_df["% Success of Out of KB"] = out_of_kb_success_per

            return brand_df.sort_values(by='Total Response', ascending=False)
            # return brand_df
        except Exception as e:
            print(f"An error occurred while processing Alerts: {str(e)}")

    def accuracy_update(self):
        final_df = self.alert_formatting()
        final_df = final_df.fillna('').astype(str)
        # print(final_df)
        sheet_name = f"Genie Accuracy Alert {self.current_date}"
        link = create_editable_sheet(final_df, sheet_name)
        text = f'Date: {self.current_date}\nPlease find Daily Genie Accuracy Alert\nLink: {link}'
        send_to_g_chat(data=text)



if __name__ == '__main__':
    controller = DailyAccuracy()
    controller.accuracy_update()