package sqltor

import (
	. "github.com/smartystreets/goconvey/convey"
	"testing"
)

func TestCreateFilter(t *testing.T) {
	Convey("Testing the creation of new filter with the use of helpers", t, func() {
		filter := NewFilter()
		filter.
			Select("t1.id").From("t1").
			Join("t2").On("t1.t2_id = t2.id").
			Where("t2.col > 7", "t1.col > 8")

		expected := &Filter{
			SelectedColumns: []string{"t1.id"},
			Tables:          []string{"t1", "t2"},
			JoinsOn:         []string{"t1.t2_id = t2.id"},
			WhereClauses:    []string{"t2.col > 7", "t1.col > 8"},
		}

		So(filter, ShouldResemble, expected)

		filter = NewFilter()
		filter.Select("distinct series.id").From("series").
			Join("game").On("series.id = game.series_id").
			Join("game_team").On("game.id = game_team.game_id").
			Join("team").On("team.id = game_team.team_id").
			Join("country").On("country.id = team.country").
			Where("country.code = 'RU'")

		expected = &Filter{
			SelectedColumns: []string{"distinct series.id"},
			Tables:          []string{"series", "game", "game_team", "team", "country"},
			JoinsOn:         []string{"series.id = game.series_id", "game.id = game_team.game_id", "team.id = game_team.team_id", "country.id = team.country"},
			WhereClauses:    []string{"country.code = 'RU'"},
		}

		So(filter, ShouldResemble, expected)
	})

}

func TestCreateQuery(t *testing.T) {
	Convey("Testing creating query", t, func() {
		f1 := Filter{
			SelectedColumns: []string{"t1.id"},
			Tables:          []string{"t1", "t2"},
			JoinsOn:         []string{"t1.t2_id = t2.id"},
			WhereClauses:    []string{"t2.col > ?", "t1.col2 = 'colcol'"},
		}
		f2 := Filter{
			SelectedColumns: []string{"t1.id"},
			Tables:          []string{"t1"},
			JoinsOn:         nil,
			WhereClauses:    []string{"t1.col > ?"},
		}
		f3 := Filter{
			SelectedColumns: []string{"t1.id"},
			Tables:          []string{"t1", "t3", "t4"},
			JoinsOn:         []string{"t1.t3_id = t3.id", "t3.t4_id = t4.id"},
			WhereClauses:    []string{"t4.col = ?"},
		}

		RegisterFilter("f1", &f1)
		RegisterFilter("f2", &f2)
		RegisterFilter("f3", &f3)

		res, args, err := CreateQuery([]string{"f1", "f2", "f3"}, [][]interface{}{{1}, {2}, {3}})
		expected := "select t1.id from t1 join t3 on t1.t3_id = t3.id join t4 on t3.t4_id = t4.id join t2 on t1.t2_id = t2.id where t2.col > ? and t1.col2 = 'colcol' and t1.col > ? and t4.col = ?"
		So(err, ShouldBeNil)
		So(res, ShouldEqual, expected)
		So(len(args), ShouldEqual, 3)
		So(args, ShouldResemble, []interface{}{1, 2, 3})

		f4 := Filter{
			SelectedColumns: []string{"distinct series.id"},
			Tables:          []string{"series", "tournament"},
			JoinsOn:         []string{"series.tournament_id = tournament.id"},
			WhereClauses:    []string{"tournament.tier = ?"},
		}

		f5 := Filter{
			SelectedColumns: []string{"distinct series.id"},
			Tables:          []string{"series", "game", "game_team", "team", "country"},
			JoinsOn:         []string{"series.id = game.series_id", "game.id = game_team.game_id", "team.id = game_team.team_id", "country.id = team.country"},
			WhereClauses:    []string{"country.code in (?)"},
		}

		RegisterFilter("f4", &f4)
		RegisterFilter("f5", &f5)

		res, args, err = CreateQuery([]string{"f4", "f5"}, [][]interface{}{{1}, {"RU", "EN"}})
		expected = "select distinct series.id from series join game on series.id = game.series_id join game_team on game.id = game_team.game_id join team on team.id = game_team.team_id join country on country.id = team.country join tournament on series.tournament_id = tournament.id where tournament.tier = ? and country.code in (?, ?)"
		So(err, ShouldBeNil)
		So(res, ShouldEqual, expected)
		So(len(args), ShouldEqual, 3)
		So(args, ShouldResemble, []interface{}{1, "RU", "EN"})

		f6 := Filter{
			SelectedColumns: []string{"table.column"},
			Tables:          []string{"table"},
			JoinsOn:         []string{},
			WhereClauses:    []string{"table.column = x"},
		}

		RegisterFilter("f6", &f6)
		res, args, err = CreateQuery([]string{"f5", "f6"}, [][]interface{}{{"RU"}})
		So(res, ShouldBeEmpty)
		So(args, ShouldBeNil)
		So(err, ShouldNotBeNil)

		f7 := Filter{
			SelectedColumns: []string{"table.column"},
			Tables:          []string{},
			JoinsOn:         []string{},
			WhereClauses:    []string{"table.column = x"},
		}

		RegisterFilter("f7", &f7)
		res, args, err = CreateQuery([]string{"f7"}, nil)
		So(res, ShouldBeEmpty)
		So(args, ShouldBeNil)
		So(err, ShouldNotBeNil)

		res, args, err = CreateQuery([]string{"f8", "f6"}, nil)
		So(res, ShouldBeEmpty)
		So(args, ShouldBeNil)
		So(err, ShouldNotBeNil)

		res, args, err = CreateQuery([]string{"f4", "f5"}, [][]interface{}{{1}})
		So(res, ShouldBeEmpty)
		So(args, ShouldBeNil)
		So(err, ShouldNotBeNil)

		res, args, err = CreateQuery([]string{"f4", "f5"}, [][]interface{}{{1}, {2}, {3}})
		So(res, ShouldBeEmpty)
		So(args, ShouldBeNil)
		So(err, ShouldNotBeNil)
	})
}
