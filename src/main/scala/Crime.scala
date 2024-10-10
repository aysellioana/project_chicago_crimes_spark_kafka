
case class Crime(
                  id: String,
                  case_number: String,
                  date: String,
                  block: String,
                  iucr: String,
                  primary_type: String,
                  description: String,
                  location_description: String,
                  arrest: Boolean,
                  domestic: Boolean,
                  beat: String,
                  district: String,
                  ward: String,
                  community_area: String,
                  fbi_code: String,
                  year: String
                )
