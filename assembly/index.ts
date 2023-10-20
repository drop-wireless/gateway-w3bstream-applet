// The entry file of your WebAssembly module.

// export function add(a: i32, b: i32): i32 {
//   return a + b;
// }
import { Date } from "assemblyscript/std/assembly/date"
import { GetDataByRID, Log, ExecSQL, JSON, QuerySQL } from "@w3bstream/wasm-sdk";
import { String, Int64, Int32, Float64 } from "@w3bstream/wasm-sdk/assembly/sql"
// import { JSON } from "json-as/assembly";
//export alloc for w3bstream vm
export { alloc } from "@w3bstream/wasm-sdk";


export function log(rid: i32): i32 {
  const message = GetDataByRID(rid);
  Log("log: " + rid.toString() + ": " + message);
  return 0;
}
export function autoCollect(rid: i32): i32 {
  const message = GetDataByRID(rid);
  Log("autoCollect: " + rid.toString() + ": " + message);
  return 0;
}
// replaced with autoCollect DePINScan
export function updateLocation(rid: i32): i32 {
  const payload: JSON.Obj = (JSON.parse(GetDataByRID(rid)) as JSON.Obj);
  const gateway_id = payload.getString("gateway_id") || new JSON.Str("");
  const location = payload.getObj("location") || new JSON.Obj();
  if (gateway_id.toString().length) {
    // Log("updateLocation: " + gateway_id.toString() + " location: " + location.toString());

    const deleteRecord = ExecSQL(
      `DELETE FROM gateway_location WHERE gateway_id = ?;`,
      [new String(gateway_id.toString())]
    );

    const result = ExecSQL(
      `INSERT INTO gateway_location (gateway_id,location) VALUES (?,?);`,
      [
        new String(gateway_id.toString()),
        new String(location.stringify())
      ]
    );
    Log("location inserted: " + result.toString() + ": " + gateway_id.toString());
    return result;
  }
  else {
    Log("updateLocation:invalid payload");
    return 1;
  }
}
export function heartbeat(rid: i32): i32 {
  const payload: JSON.Obj = (JSON.parse(GetDataByRID(rid)) as JSON.Obj);
  const heartbeat_id = payload.getInteger("heartbeat_id") || new JSON.Integer(0);
  const gateway_id = payload.getString("gateway_id") || new JSON.Str("");
  const heartbeat = payload.getObj("heartbeat") || new JSON.Obj();

  if (gateway_id.toString().length && heartbeat_id.valueOf() > 0) {
    const result = ExecSQL(
      `INSERT INTO gateway_heartbeats (heartbeat_id, gateway_id, heartbeat) VALUES (?,?,?);`,
      [
        new Int64(heartbeat_id.valueOf()),
        new String(gateway_id.toString()),
        new String(heartbeat.stringify())
      ]
    );
    Log("heartbeat inserted: " + result.toString() + ": " + heartbeat_id.toString());
    return result;
  }
  else {
    Log("heartbeat:invalid payload");
    return 1;
  }
}

export function advertisement(rid: i32): i32 {
  const payload: JSON.Obj = (JSON.parse(GetDataByRID(rid)) as JSON.Obj);
  const gateway_id = payload.getString("gateway_id") || new JSON.Str("");
  const seconds_played = payload.getInteger("seconds_played") || new JSON.Integer(0);
  const dwin_earned = payload.getFloat("dwin_earned") || new JSON.Float(0);
  const played = payload.getInteger("played") || new JSON.Integer(0);
  const from_date = payload.getString("from_date") || new JSON.Str("");
  const to_date = payload.getString("to_date") || new JSON.Str("");

  if (gateway_id.toString().length) {
    const result = ExecSQL(
      `INSERT INTO gateway_advertisement (gateway_id,played,seconds_played,dwin_earned,from_date,to_date) VALUES (?,?,?,?,?,?);`,
      [
        new String(gateway_id.toString()),
        new Int64(played.valueOf()),
        new Int64(seconds_played.valueOf()),
        new Float64(dwin_earned.valueOf()),
        new String(from_date.toString()),
        new String(to_date.toString()),
      ]
    );
    Log("ad inserted: " + result.toString() + ": " + gateway_id.toString());
    return result;
  }
  else {
    Log("ad:invalid payload");
    return 1;
  }
}