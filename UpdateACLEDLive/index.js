require('isomorphic-fetch');
require('isomorphic-form-data');

const moment = require('moment');
const featureService = require('@esri/arcgis-rest-feature-service');
const restAuth = require('@esri/arcgis-rest-auth');

const liveFeatureServiceUrl = 'https://services.arcgis.com/LG9Yn2oFqZi5PnO5/arcgis/rest/services/Armed_Conflict_Location_Event_Data_ACLED/FeatureServer/0';

let _SESSION;
let _ACLEDDATA;

const translateToFeatureJson = (data) => {
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

const getLiveAcledData = () => {
  const fourteenDaysAgo = moment().subtract(14, 'days').format('YYYY-MM-DD');
  const apiUrl = `https://api.acleddata.com/acled/read?event_date=${fourteenDaysAgo}&event_date_where=%3E=&limit=0&terms=accept`;

  console.log('requesting data from ACLED API ..');
  console.log(`ACLED API request URL :: ${apiUrl}`);

  return fetch(apiUrl)
    .then(response => response.json())
    .then(responseData => {
      if (!responseData) {
        throw new Error('no response data returned from ACLED API');
      } else if (responseData.count === 0 || responseData.data.length === 0) {
        throw new Error('no features from ACLED API returned. exiting ..');
      } else {
        _ACLEDDATA = translateToFeatureJson(responseData.data);
        return Promise.resolve();
      }
    });
};

const deleteLiveFeatures = () => {
  console.log('deleting features ..');
  const deleteParams = {
    url: liveFeatureServiceUrl,
    params: { where: '1=1' },
    authentication: _SESSION
  };
  return featureService.deleteFeatures(deleteParams)
    .catch((error) => {
      throw new Error(error);
    });
};

const insertLiveFeatures = () => {
  console.log('inserting features ..');
  const addParams = {
    url: liveFeatureServiceUrl,
    adds: _ACLEDDATA,
    authentication: _SESSION
  };
  return featureService.addFeatures(addParams)
    .catch((error) => {
      throw new Error(error);
    });
};

const initAuth = () => {
  return new Promise((resolve, reject) => {
    _SESSION = new restAuth.UserSession({
      username: process.env.SERVICE_USER,
      password: process.env.SERVICE_PASS
    });

    if (!_SESSION) {
      reject(new Error('unable to get authentication setup'));
    }

    resolve();
  });
};

const processEvent = () => {
  initAuth()
    .then(getLiveAcledData)
    .then(deleteLiveFeatures)
    .then(insertLiveFeatures)
    .then(response => {
      let message = '';
      if (response && response.addResults) {
        message = `successfully added ${response.addResults.length}`;
      } else {
        message = 'unable to insert features';
      }
      return message;
    })
    .catch(error => {
      return error;
    });
};

module.exports = async function (context, myTimer) {
    var timeStamp = new Date().toISOString();
    
    if (myTimer.IsPastDue)
    {
        context.log('JavaScript is running late!');
    }

    context.log('ACLED Update Initiated', timeStamp);     

    const result = processEvent();

    context.log('ACLED Live update completed', result);
};