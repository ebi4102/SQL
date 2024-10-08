CREATE TABLE vote14(
state VARCHAR(50),
pc_name VARCHAR(300),
candidate VARCHAR(300),
sex VARCHAR(10),
age INTEGER,
category VARCHAR(100),
party VARCHAR(100),
party_symbol VARCHAR(200),
general_votes INTEGER,
postal_votes INTEGER,
total_votes INTEGER,
total_electors INTEGER
);

COPY vote14 FROM 'D:\Analytics Dataset\datasets\2014.csv' WITH CSV HEADER;

SELECT * FROM vote14;

CREATE TABLE vote19(
state VARCHAR(50),
pc_name VARCHAR(300),
candidate VARCHAR(300),
sex VARCHAR(10),
age INTEGER,
category VARCHAR(100),
party VARCHAR(100),
party_symbol VARCHAR(200),
general_votes INTEGER,
postal_votes INTEGER,
total_votes INTEGER,
total_electors INTEGER
);

COPY vote19 FROM 'D:\Analytics Dataset\datasets\2019.csv' WITH CSV HEADER;

SELECT * FROM vote19;

--1. Top 5 / Bottom 5 Constituencies (2014 and 2019) in terms of Voter Turnout Ratio

--2014:
-- Top 5 constituencies by voter turnout ratio (2014)
SELECT pc_name, state, (total_votes*100/total_electors) AS voter_turnout_ratio
FROM vote14
ORDER BY voter_turnout_ratio DESC
LIMIT 5;
--Bottom 5 constituencies by voter turnout ratio (2014)
SELECT pc_name, state, (total_votes*100/total_electors) AS voter_turnout_ratio
FROM vote14
ORDER BY voter_turnout_ratio ASC
LIMIT 5;

--2019:
-- Top 5 constituencies by voter turnout ratio (2019)
SELECT pc_name, state, (total_votes*100/total_electors) AS voter_turnout_ratio
FROM vote19
ORDER BY voter_turnout_ratio DESC
LIMIT 5;
-- Bottom 5 constituencies by voter turnout ratio (2019)
SELECT pc_name, state, (total_votes*100/total_electors) AS voter_turnout_ratio
FROM vote19
ORDER BY voter_turnout_ratio ASC
LIMIT 5;

--2. Top 5 / Bottom 5 States (2014 and 2019) in terms of Voter Turnout Ratio

--2014:
-- Top 5 states by voter turnout ratio (2014)
SELECT state, (SUM(total_votes)*100 / SUM(total_electors)) AS voter_turnout_ratio
FROM vote14
GROUP BY state
ORDER BY voter_turnout_ratio DESC
LIMIT 5;
-- Bottom 5 states by voter turnout ratio (2014)
SELECT state, (sum(total_votes)*100/sum(total_electors)) AS voter_turnout_ratio
FROM vote14
GROUP BY state
ORDER BY voter_turnout_ratio ASC
LIMIT 5;

--2019:
-- Top 5 states by voter turnout ratio (2019)
SELECT state, (sum(total_votes)*100/sum(total_electors)) AS voter_turnout_ratio
FROM vote19
GROUP BY state 
ORDER BY voter_turnout_ratio DESC 
LIMIT 5;
-- Bottom 5 states by voter turnout ratio (2019)
SELECT state, (sum(total_votes)*100/sum(total_electors)) AS voter_turnout_ratio
FROM vote19
GROUP BY state
ORDER BY voter_turnout_ratio ASC
LIMIT 5;

--3. Constituencies that Elected the Same Party in 2014 and 2019, Ranked by 2019 Winning Percentage

WITH winners_2014 AS(
SELECT state, pc_name, party, max(total_votes) AS votes_2014
FROM vote14
GROUP BY state, pc_name, party
),
winners_2019 AS(
SELECT state, pc_name, party, max(total_votes) AS votes_2019
FROM vote19
GROUP BY state, pc_name, party
)
SELECT w19.state, w19.pc_name, w19.party,(w19.votes_2019*100/v19.total_votes) AS vote_percentage_2019
FROM winners_2014 AS w14
JOIN winners_2019 AS w19 ON w14.state=w19.state AND w14.pc_name=w19.pc_name AND w14.party=w19.party
JOIN vote19 AS v19 ON w19.pc_name=v19.pc_name AND w19.state=v19.state
ORDER BY vote_percentage_2019 DESC;

--4. Constituencies that Voted for Different Parties in 2014 and 2019 (Top 10 by Difference)

WITH winners_2014 AS(
SELECT state, pc_name, party, max(total_votes) AS votes_2014, (max(total_votes)*100.0/max(total_electors)) AS vote_percentage_2014
FROM vote14
GROUP BY state,pc_name, party 
),
winners_2019 AS(
SELECT state,pc_name, party, max(total_votes) AS votes_2019, (max(total_votes)*100.0/max(total_electors)) AS vote_percentage_2019
FROM vote19
GROUP BY state, pc_name, party 
)
SELECT w19.state, w19.pc_name,w14.party AS party_2014, w19.party AS party_2019,(w19.vote_percentage_2019 - w14.vote_percentage_2014) AS vote_diff
FROM winners_2014 AS w14
JOIN winners_2019 AS w19 ON w14.state=w19.state AND w14.pc_name=w19.pc_name
WHERE w14.party <> w19.party
ORDER BY vote_diff DESC
LIMIT 10; 

--5. Top 5 Candidates by Margin Difference with Runners in 2014 and 2019
--2014:
WITH margin_2014 AS(
SELECT state,pc_name,candidate,party, (total_votes - COALESCE(lag(total_votes) OVER (PARTITION BY state, pc_name ORDER BY total_votes DESC),0)) AS margin_2014
FROM vote14
ORDER BY pc_name, margin_2014 DESC
)
SELECT * FROM margin_2014
ORDER BY margin_2014 DESC
LIMIT 5;
--2019;
WITH margin_2019 AS(
SELECT state, pc_name, candidate, party, (total_votes - COALESCE(lag(total_votes) OVER (PARTITION BY state, pc_name ORDER BY total_votes DESC),0)) AS margin_2019
FROM vote19
ORDER BY pc_name, margin_2019 DESC
)
SELECT * FROM margin_2019
ORDER BY margin_2019 DESC
LIMIT 5; 

--Comparing Margin Differences Between 2014 and 2019
WITH margin_2014 AS (
    SELECT state, pc_name, candidate, party, 
           (total_votes - COALESCE(LAG(total_votes) OVER (PARTITION BY state, pc_name ORDER BY total_votes DESC), 0)) AS margin_2014
    FROM vote14
),
margin_2019 AS (
    SELECT state, pc_name, candidate, party, 
           (total_votes - COALESCE(LAG(total_votes) OVER (PARTITION BY state, pc_name ORDER BY total_votes DESC), 0)) AS margin_2019
    FROM vote19
)
SELECT m19.state, m19.pc_name, 
       m14.candidate AS candidate_2014, m19.candidate AS candidate_2019, 
       m14.party AS party_2014, m19.party AS party_2019,
       m14.margin_2014, m19.margin_2019,
       (m19.margin_2019 - m14.margin_2014) AS margin_difference
FROM margin_2014 m14
JOIN margin_2019 m19 ON m14.state = m19.state AND m14.pc_name = m19.pc_name
ORDER BY margin_difference DESC
LIMIT 5;

--6. % Split of Votes of Parties Between 2014 and 2019 at National Level
WITH vote_share_2014 AS (
SELECT party, (sum(total_votes)*100.0/(SELECT sum(total_votes) FROM vote14)) AS vote_share_2014
FROM vote14
GROUP BY party
),
vote_share_2019 AS (
SELECT party, (sum(total_votes)*100.0/(SELECT sum(total_votes) FROM vote19)) AS vote_share_2019
FROM vote19
GROUP BY party
)
SELECT COALESCE(vs14.party,vs19.party) AS party,
COALESCE(vs14.vote_share_2014, 0) AS vote_share_2014,
COALESCE(vs19.vote_share_2019, 0) AS vote_share_2019,
(COALESCE(vs19.vote_share_2019, 0) - COALESCE(vs14.vote_share_2014, 0)) AS vote_share_difference
FROM vote_share_2014 AS vs14
FULL OUTER JOIN vote_share_2019 AS vs19 ON vs14.party=vs19.party
ORDER BY vote_share_difference DESC;

--7. % Split of Votes of Parties Between 2014 and 2019 at State Level
WITH vote_share_2014 AS(
SELECT state,party, (sum(total_votes)*100.0 / (SELECT sum(total_votes) FROM vote14 WHERE state = v.state)) AS vote_share_2014
FROM vote14 AS v 
GROUP BY state, party
),
vote_share_2019 AS(
SELECT state, party, (sum(total_votes)*100.0 / (SELECT sum(total_votes) FROM vote19 WHERE state = v.state)) AS vote_share_2019
FROM vote19 AS v 
GROUP BY state, party
)
SELECT COALESCE(vs14.state,vs19.state) AS state,
COALESCE(vs14.party,vs19.party) AS party,
COALESCE(vs14.vote_share_2014,0) AS vote_share_2014,
COALESCE(vs19.vote_share_2019,0) AS vote_share_2019,
(COALESCE (vs19.vote_share_2019,0) - COALESCE (vs14.vote_share_2014,0)) AS vote_share_difference
FROM vote_share_2014 AS vs14
FULL OUTER JOIN vote_share_2019 AS vs19 ON vs14.state=vs19.state AND vs14.party=vs19.party
ORDER BY state, vote_share_difference DESC;

--8. Top 5 Constituencies for Two Major National Parties (BJP and IND) Where They Gained Vote Share in 2019 Compared to 2014
WITH vote_gain AS(
SELECT v14.pc_name, v14.state, v14.party, (v19.total_votes - v14.total_votes) AS vote_difference
FROM vote14 AS v14 
JOIN vote19 AS v19 ON v14.pc_name=v19.pc_name AND v14.party=v19.party
WHERE v14.party IN ('BJP','IND')
)
SELECT * FROM vote_gain
ORDER BY vote_difference DESC
LIMIT 5;

--9. Top 5 Constituencies for Two Major National Parties (BJP and IND) Where They Gained Vote Share in 2019 Compared to 2014
WITH vote_gain AS(
SELECT v14.pc_name, v14.state, v14.party, (v19.total_votes - v14.total_votes) AS vote_difference
FROM vote14 AS v14 
JOIN vote19 AS v19 ON v14.pc_name=v19.pc_name AND v14.party=v19.party
WHERE v14.party IN ('BJP','IND')
)
SELECT * FROM vote_gain
ORDER BY vote_difference ASC
LIMIT 5;

--10.Constituency with Most IND Votes
-- 2014
SELECT pc_name, state, MAX(total_votes) AS ind_votes
FROM vote14
WHERE party = 'IND'
GROUP BY pc_name, state
ORDER BY ind_votes DESC
LIMIT 1;

-- 2019
SELECT pc_name, state, MAX(total_votes) AS ind_votes
FROM vote19
WHERE party = 'IND'
GROUP BY pc_name, state
ORDER BY ind_votes DESC
LIMIT 1;

--11. Constituencies that changed their winning party between 2014 and 2019
WITH winners_2014 AS (
    SELECT state, pc_name, party, MAX(total_votes) AS votes_2014
    FROM vote14
    GROUP BY state, pc_name, party
),
winners_2019 AS (
    SELECT state, pc_name, party, MAX(total_votes) AS votes_2019
    FROM vote19
    GROUP BY state, pc_name, party
)
SELECT w14.state, w14.pc_name, w14.party AS party_2014, w19.party AS party_2019
FROM winners_2014 w14
JOIN winners_2019 w19 ON w14.state = w19.state AND w14.pc_name = w19.pc_name
WHERE w14.party <> w19.party;

