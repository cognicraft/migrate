package migrate

import "testing"
import "reflect"

func TestStatements(t *testing.T) {
	tests := []struct {
		name       string
		script     string
		statements []string
	}{
		{
			"1",
			"CREATE TABLE foo (bar PRIMARY KEY);",
			[]string{"CREATE TABLE foo (bar PRIMARY KEY);"},
		},
		{
			"2",
			`
			CREATE TABLE foo (bar PRIMARY KEY);
			CREATE TABLE bar (baz PRIMARY KEY);
			`,
			[]string{"CREATE TABLE foo (bar PRIMARY KEY);", "CREATE TABLE bar (baz PRIMARY KEY);"},
		},
		{
			"3",
			`
			CREATE TRIGGER IF NOT EXISTS stream_version AFTER INSERT ON events
			FOR EACH ROW
			BEGIN
			UPDATE streams SET version = NEW.streamIndex+1 WHERE id=NEW.streamID;
			END;
			`,
			[]string{"CREATE TRIGGER IF NOT EXISTS stream_version AFTER INSERT ON events\nFOR EACH ROW\nBEGIN\nUPDATE streams SET version = NEW.streamIndex+1 WHERE id=NEW.streamID;\nEND;"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Statements(test.script)
			if len(got) != len(test.statements) {
				t.Errorf("statement count does not match: want: %d, got %d", len(test.statements), len(got))
			}
			if !reflect.DeepEqual(test.statements, got) {
				t.Errorf("want: %#v, got: %#v", test.statements, got)
			}
		})
	}
}
