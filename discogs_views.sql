select   rt.* , 
json_agg(
jsonb_build_object(
'artist_id',rta.artist_id,
'artist_name', rta.artist_name,
'position', rta."position",
'join_string',rta.join_string,
'role', rta."role", 
'anv', rta.anv,
'artistdetails',ajv.artistdetails 
) 
) as artists 
 from 
release_track rt 
left join release_track_artist rta on rt.id  = CAST (rta.track_id  AS INTEGER)
left join artist_json_view ajv on rta.artist_id = ajv.artist_id 
group by rt.id  ;



--select r.* ,json_agg(distinct rg.genre) , json_agg(distinct rs."style"),
--json_agg(distinct jsonb_build_object('artist_id',ra.artist_id,'artist_name', ra.artist_name,'position', ra."position",'join_string', ra.join_string)    ) ,
--json_agg(distinct jsonb_build_object('track_id', rt.track_id, 'sequence',rt."sequence",'title', rt.title)) 
--from 
--release r, release_genre rg,release_style rs , release_artist ra , release_track rt  where
-- r.id =rg.release_id 
--and r.id = rs.release_id
--and r.id = ra.release_id 
--and r.id = rt.release_id 
--group by r.id ;


select r.* ,json_agg(distinct rg.genre) as generes, json_agg(distinct rs."style") as styles,
json_agg(distinct jsonb_build_object('artist_id',ra.artist_id,'artist_name', ra.artist_name,'position', ra."position",'join_string', ra.join_string)    ) as releaseartits,
json_agg(distinct jsonb_build_object('track_id', rt.track_id, 'sequence',rt."sequence",'title', rt.title)) as tracks_only,
json_agg(tr.*) as tracks
from 
release r
left join release_genre rg on r.id =rg.release_id 
left join release_style rs on r.id = rs.release_id
left join release_artist ra on r.id = ra.release_id 
left join release_track rt on  r.id = rt.release_id  
left join (
select   rt.* , 
json_agg(jsonb_build_object('artist_id',rta.artist_id,'artist_name', rta.artist_name,'position', rta."position",'join_string',rta.join_string,'role', rta."role", 'anv', rta.anv) ) as artist 
 from 
release_track rt left join release_track_artist rta   
on rt.id  = CAST (rta.track_id  AS INTEGER)
group by rt.id 
) tr on r.id = tr.release_id
group by r.id ;
  


-- Artist details 

select a.id as artist_id , a."name" as "name" , a.realname as real_name  , a.profile ,json_agg(distinct aa.alias_name) as aliases, json_agg(distinct an."name") as namevariations,  json_agg(distinct au.url) as urls,
 json_agg( distinct gm.member_name ) as members, json_agg( distinct gm.member_artist_id  ) as members_id
--json_agg(distinct jsonb_build_object('artist_id',ra.artist_id,'artist_name', ra.artist_name,'position', ra."position",'join_string', ra.join_string)    ) as releaseartits
from 
artist a  
left join artist_alias aa on aa.artist_id = a.id 
left join artist_namevariation an on an.artist_id = a.id 
left join artist_url au on au.artist_id = a.id 
left join group_member gm ON gm.group_artist_id =a.id 
group by a.id ;


CREATE MATERIALIZED VIEW artist_view AS
select a.id as artist_id , a."name" as "name" , a.realname as real_name  , a.profile ,json_agg(distinct aa.alias_name) as aliases, json_agg(distinct an."name") as namevariations,  json_agg(distinct au.url) as urls,
 json_agg( distinct gm.member_name ) as members, json_agg( distinct gm.member_artist_id  ) as members_id
from 
artist a  
left join artist_alias aa on aa.artist_id = a.id 
left join artist_namevariation an on an.artist_id = a.id 
left join artist_url au on au.artist_id = a.id 
left join group_member gm ON gm.group_artist_id =a.id 
group by a.id 
WITH DATA;

-- Artist details Json View
CREATE MATERIALIZED VIEW artist_json_view AS
select a.id as artist_id ,
jsonb_build_object(
'artist_id',a.id ,
'name', a."name" ,
'real_name', a.realname  ,
'profile',a.profile ,
'aliases', json_agg(distinct aa.alias_name),
'namevariations', json_agg(distinct an."name"),
'urls', json_agg(distinct au.url),
'members', json_agg( distinct gm.member_name ),
'members_id', json_agg( distinct gm.member_artist_id  )) as artistdetails
from 
artist a  
left join artist_alias aa on aa.artist_id = a.id 
left join artist_namevariation an on an.artist_id = a.id 
left join artist_url au on au.artist_id = a.id 
left join group_member gm ON gm.group_artist_id =a.id 
group by a.id 
WITH DATA;

