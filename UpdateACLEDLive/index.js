require('isomorphic-fetch');
require('isomorphic-form-data');

const moment = require('moment');
const { deleteFeatures, addFeatures } = require('@esri/arcgis-rest-feature-layer');
const restAuth = require('@esri/arcgis-rest-auth');

const liveFeatureServiceUrl = 'https://services.arcgis.com/LG9Yn2oFqZi5PnO5/arcgis/rest/services/Armed_Conflict_Location_Event_Data_ACLED/FeatureServer/0';

const translateToFeatureJson = data => {
  return data.map(event => {
    return {
      geometry: {
        x: parseFloat(event.longitude),
        y: parseFloat(event.latitude)
      },
      attributes: {
        data_id: parseInt(event.data_id),
        iso: event.iso,
        event_id_cnty: event.event_id_cnty,
        event_id_no_cnty: event.event_id_no_cnty,
        event_date: moment(event.event_date).format('YYYY-MM-DD'),
        year: parseInt(event.year),
        time_precision: event.time_precision,
        event_type: event.event_type,
        actor1: event.actor1,
        assoc_actor_1: event.assoc_actor_1,
        inter1: event.inter1,
        actor2: event.actor2,
        assoc_actor_2: event.assoc_actor_2,
        inter2: event.inter2,
        interaction: event.interaction,
        region: event.region,
        country: event.country,
        admin1: event.admin1,
        admin2: event.admin2,
        admin3: event.admin3,
        location: event.location,
        latitude: parseFloat(event.latitude),
        longitude: parseFloat(event.longitude),
        geo_precision: event.geo_precision,
        source: event.source,
        source_scale: event.source_scale,
        notes: event.notes,
        fatalities: parseInt(event.fatalities),
        timestamp: event.timestamp,
        iso3: event.iso3
      }
    };
  });
};

const deleteLiveFeatures = async session => {
  const deleteParams = {
    url: liveFeatureServiceUrl,
    params: { where: '1=1' },
    authentication: session
  };
  return deleteFeatures(deleteParams);
};

const insertLiveFeatures = (newData, session) => {
  console.log('inserting features ..');
  const addParams = {
    url: liveFeatureServiceUrl,
    features: newData,
    authentication: session
  };
  return addFeatures(addParams);
};

const initAuth = async () => {
  return new Promise((resolve, reject) => {
    let session = null;
    try {
      session = new restAuth.UserSession({
        username: process.env.SERVICE_USER,
        password: process.env.SERVICE_PASS
      });
      resolve(session);
    } catch (error) {
      reject(new Error('unable to get authentication setup'));  
    }
  });
};

const getLiveAcledData = async () => {
  const fourteenDaysAgo = moment('2019-08-01').subtract(14, 'days').format('YYYY-MM-DD');
  const apiUrl = `https://api.acleddata.com/acled/read?event_date=${fourteenDaysAgo}&event_date_where=%3E=&limit=0&terms=accept`

  console.log('requesting data from ACLED API ..');
  console.log(`ACLED API request URL :: ${apiUrl}`);

  return fetch(apiUrl)
    .then(response => response.json())
    .then(responseData => {
      if (!responseData) {
        throw new Error('no response data returned from ACLED API');
      } else if (responseData.count === 0 || responseData.data.length === 0) {
        return [];
      } else {
        return translateToFeatureJson(responseData.data);        
      }
    });
};


module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    if (myTimer.IsPastDue) {
        context.log('JavaScript is running late!');
    }

    context.log('ACLED Update Initiated', timeStamp);     

    const sessionInfo = await initAuth();

    let newData = null;
    try {
      newData = await getLiveAcledData();
      if (newData.length === 0) {
        context.log('ACLED Update Completed. No data returned from API'); 
        return context.done();
      }
    } catch (error) {
      console.log(error); 
    }
    
    let deleteResponse = null;
    try {
      deleteResponse = await deleteLiveFeatures(sessionInfo);
      console.log(`deleted ${deleteResponse.deleteResults.length} features`); 
    } catch (error) {
      console.log(error); 
    }

    let addResponse = null;
    try {
      addResponse = await insertLiveFeatures(newData, sessionInfo);
    } catch (error) {
      console.log(error); 
    }

    context.log(`successfully added ${addResponse.addResults.length} features`);
    context.log('ACLED Live update completed');
};