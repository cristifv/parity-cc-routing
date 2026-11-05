#pragma once


struct shrinkage_record {
  int       total_scheduled = 0;
  int       absent_vacation = 0;
  int       absent_training = 0;
  int       absent_sick = 0;
  int       absent_noshow = 0;
  int       on_break = 0;
  int       on_coaching = 0;
  int       on_aux = 0;
};
