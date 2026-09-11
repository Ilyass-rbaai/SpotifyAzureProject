class Reusable:
    def dropCol(self,df,col):
        df=df.drop(col)
        return df
