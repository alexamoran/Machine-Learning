import pandas as pd
import re
import ast

movie_cols = ['vote_count', 'runtime', 'popularity', 'original_language', 'total_different_languages', 'revenue',
              'budget', 'total_production_countries', 'total_production_companies', 'total_genres', 'Total Cast Count',
              'Male Cast Percentage', 'Total Crew Count', 'Male Crew Percentage', 'Rating > 70%', ]

google_play_cols = ['Category', 'Reviews', 'Installs', 'Type', 'Size', 'Price', 'Content Rating',
                    'Genres', 'Current Version', 'Rating > 80%']


def set_up_google_play_apps_csv():
    google_df = pd.read_csv('Raw/googleplaystore.csv')
    google_df['Current Version'] = [int(re.findall('\d', str(i))[0].strip()) if len(re.findall('\d', str(i))) > 0
                                    else '' for i in google_df['Current Ver']]
    google_df = google_df[(google_df['Rating'] >= 0)
                          & (google_df['Rating'] <= 5)
                          & (google_df['Size'] != 'Varies with device')
                          & (google_df['Current Version'] != '')]
    google_df['Rating > 80%'] = [True if i >= 4.0 else False for i in google_df['Rating']]

    google_df['Size'] = [float(str(i).replace('M', '')) if 'M' in str(i) else float(str(i).replace('k', '')) / 1000
                         for i in google_df['Size']]
    google_df['Installs'] = [float(str(i).replace('+', '').replace(',', '')) for i in google_df['Installs']]
    google_df['Price'] = [float(str(i).replace('$', '')) for i in google_df['Price']]

    cat_count = 0
    cat_dict = {}
    for i in list(google_df['Category'].unique()):
        cat_dict[i] = cat_count
        cat_count += 1
    google_df['Category'] = [cat_dict[i] for i in google_df['Category']]

    type_count = 0
    type_dict = {}
    for i in list(google_df['Type'].unique()):
        type_dict[i] = type_count
        type_count += 1
    google_df['Type'] = [type_dict[i] for i in google_df['Type']]

    rating_count = 0
    rating_dict = {}
    for i in list(google_df['Content Rating'].unique()):
        rating_dict[i] = rating_count
        rating_count += 1
    google_df['Content Rating'] = [rating_dict[i] for i in google_df['Content Rating']]

    genre_count = 0
    genre_dict = {}
    for i in list(google_df['Genres'].unique()):
        genre_dict[i] = genre_count
        genre_count += 1
    google_df['Genres'] = [genre_dict[i] for i in google_df['Genres']]

    return google_df[google_play_cols]


def set_up_movie_credits():
    credits_df = pd.read_csv('Raw/tmdb_5000_credits.csv')

    write_list = []
    for _, row in credits_df.iterrows():
        temp_row = [row['movie_id']]
        female_cast_count = 0
        male_cast_count = 0
        for item in ast.literal_eval(row['cast']):
            my_dict = dict(item)
            if my_dict['gender'] == 1:
                female_cast_count += 1
            else:
                male_cast_count += 1
        male_cast_perc = male_cast_count * 1.0 / max(1, (female_cast_count + male_cast_count))
        total_cast = female_cast_count + male_cast_count

        female_crew_count = 0
        male_crew_count = 0
        for item in ast.literal_eval(row['crew']):
            my_dict = dict(item)
            if my_dict['gender'] == 1:
                female_crew_count += 1
            else:
                male_crew_count += 1
        male_crew_perc = male_crew_count * 1.0 / max(1, (female_crew_count + male_crew_count))
        total_crew = female_crew_count + male_crew_count

        temp_row.extend([total_cast, male_cast_perc, total_crew, male_crew_perc])
        write_list.append(temp_row)

    cols = ['id', 'Total Cast Count', 'Male Cast Percentage', 'Total Crew Count', 'Male Crew Percentage']
    return pd.DataFrame(write_list, columns=cols)


def set_up_movie_data():
    movie_df = pd.read_csv('Raw/tmdb_5000_movies.csv')

    movie_df['Rating > 70%'] = [True if i >= 7.0 else False for i in movie_df['vote_average']]
    max_budget = movie_df['budget'].astype(float).max()
    max_rev = movie_df['revenue'].astype(float).max()
    max_vote_count = movie_df['vote_count'].astype(float).max()

    map_count = 0
    map_dict = {}
    for i in list(movie_df['original_language'].unique()):
        map_dict[i] = map_count
        map_count += 1
    movie_df['original_language'] = [map_dict[i] for i in movie_df['original_language']]

    write_list = []
    for _, row in movie_df.iterrows():
        total_langs = len(ast.literal_eval(row['spoken_languages']))
        revenue = row['revenue'] / max_rev
        budget = row['budget'] / max_budget
        total_prod_countries = len(ast.literal_eval(row['production_countries']))
        total_prod_companies = len(ast.literal_eval(row['production_companies']))
        total_genres = len(ast.literal_eval(row['genres']))
        vote_count = row['vote_count'] / max_vote_count

        temp_row = [row['id'], row['Rating > 70%'], vote_count, row['runtime'], row['popularity'],
                    row['original_language'], total_langs, revenue, budget, total_prod_countries,
                    total_prod_companies, total_genres]
        write_list.append(temp_row)

    cols = ['id', 'Rating > 70%', 'vote_count', 'runtime', 'popularity', 'original_language',
            'total_different_languages', 'revenue', 'budget', 'total_production_countries',
            'total_production_companies', 'total_genres', ]
    return pd.DataFrame(write_list, columns=cols)


def sample_data(df, title, var):
    true_df = df[df[var]].copy()
    false_df = df[~df[var]].copy()

    if len(false_df) > len(true_df):
        sample_df = false_df.sample(n=len(true_df)).copy()
        fin_df = sample_df.append(true_df)
    else:
        sample_df = true_df.sample(n=len(false_df)).copy()
        fin_df = sample_df.append(false_df)

    fin_df.to_csv('{0}.csv'.format(title), index=False)


def main():
    google_df = set_up_google_play_apps_csv()
    sample_data(google_df, 'google_play_data', 'Rating > 80%')

    credits_df = set_up_movie_credits()
    movie_df = set_up_movie_data()
    movie_df = movie_df.merge(credits_df, how='left', on='id')
    movie_df[movie_cols[:-1]] = movie_df[movie_cols[:-1]].fillna(0).replace('', 0).applymap(float)
    sample_data(movie_df[movie_cols], 'movie_data', 'Rating > 70%')


if __name__ == '__main__':
    main()
