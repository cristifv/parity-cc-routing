#include <gamification_engine.h>


gamification_engine::gamification_engine(kpi_thresholds thr):
  thr_(thr) {}

gamification_score gamification_engine::compute(const performance_record& record) const {
  gamification_score gs;
  gs.agent_id = record.agent_id;
  double pts = 
      0.25 * hib(record.fcr_rate,          thr_.target_fcr,         1.0)
          + 0.25 * hib((record.csat_score-1)/4.0, (thr_.target_csat-1)/4.0, 1.0)
          + 0.20 * hib(record.adherence_rate,     thr_.target_adherence,   1.0)
          + 0.10 * lib(record.avg_handle_time_s,  thr_.target_aht_s,  60.0, 400.0)
          + 0.10 * lib(record.alert_response_s,   thr_.target_alert_resp_s, 5.0, 120.0)
          + 0.10 * hib(record.quality_score,      thr_.target_quality,     1.0);

  gs.total_points = pts;
  if      (pts >= 90.0) { gs.badge_level = 3; gs.badge_name = "Platinum"; }
  else if (pts >= 75.0) { gs.badge_level = 2; gs.badge_name = "Gold";     }
  else if (pts >= 55.0) { gs.badge_level = 1; gs.badge_name = "Silver";   }
  else                  { gs.badge_level = 0; gs.badge_name = "Bronze";   }
  return gs;
}

gamification_score gamification_engine::compute_aggregate(const std::vector<performance_record>& recs) const {
  if (recs.empty()) return {};

  double tw = 0.0;

  performance_record agg{}; agg.agent_id = recs.front().agent_id;
  for (int i = 0; i < (int) recs.size(); ++i) {
    double w = 1.0 + i * 0.1; tw += w;
    agg.fcr_rate            += w * recs[i].fcr_rate;
    agg.csat_score          += w * recs[i].csat_score;
    agg.adherence_rate      += w * recs[i].adherence_rate;
    agg.avg_handle_time_s   += w * recs[i].avg_handle_time_s;
    agg.alert_response_s    += w * recs[i].alert_response_s;
    agg.calls_handled       += recs[i].calls_handled;
    agg.quality_score       += w * recs[i].quality_score;
  }

  agg.fcr_rate/=tw;
  agg.csat_score/=tw;
  agg.adherence_rate/=tw;
  agg.avg_handle_time_s/=tw;
  agg.alert_response_s/=tw;
  agg.quality_score/=tw;
    
  return compute(agg);
}
