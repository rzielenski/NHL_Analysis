# NHL Analysis

This project is an attempt to push forward the accuracy of game prediction in hockey

## Current state

This project is an assortment of machine learning algorithms that have been tested in predicting the outcome of a hockey game (whether during or before the game)
Not all files are in a working state as I pushed this in a rush to get it out of google drive (yes we were using google drive for code storage lol)

If you have any questions email me at rpzielenski@gmail.com

## Future

The main focus of this repo has shifted to creating a transformer pipeline to predict the outcome of a game from an arbitrary point through next play prediction

By treating plays as tokens and training on 15 years of play by play data from [NHL API](https://github.com/Zmalski/NHL-API-Reference) we theoretically can predict the remander of the game given the context of the 2 team's stats
