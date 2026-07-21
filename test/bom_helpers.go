package test

import (
	"fmt"

	"github.com/salesforce/converge/examples/demos/classic/classicbom"
)

// SynthFDName is the single FD name produced by synthBOMNamed. Tests use
// this to construct expected resource names like "<fd>/<team>".
const SynthFDName = "synth"

// synthBOMNamed returns a ClassicBOMSpec with one FD ("synth") containing
// numTeams synthetic teams. Resource counts follow the FD-aware model:
//
//	1 FD-tgw account + 1 TGW + numTeams * (account + vpc + route)
//	= 3*numTeams + 2 resources (use expectedResources(numTeams)).
func synthBOMNamed(name string, numTeams int) classicbom.ClassicBOMSpec {
	teams := make([]*classicbom.ServiceTeam, numTeams)
	for i := range numTeams {
		teams[i] = &classicbom.ServiceTeam{
			Name: fmt.Sprintf("team-%05d", i),
		}
	}
	return classicbom.ClassicBOMSpec{
		DeploymentInstance: &classicbom.DeploymentInstance{
			Name: name,
			FunctionalDomains: []*classicbom.FunctionalDomain{
				{
					Name:              SynthFDName,
					AccountsOnly:      false,
					AWSTransitGateway: &classicbom.TransitGW{Enabled: true},
					ServiceTeams:      teams,
				},
			},
		},
	}
}

// expectedResources returns the resource count for a synth-FD spec
// with numTeams teams: 3*numTeams (account+vpc+route per team) + 2
// (FD-level tgw-host account + the TGW resource itself).
func expectedResources(numTeams int) int {
	return numTeams*3 + 2
}

// expectedTeamPrefix returns the per-resource name prefix for synth-FD
// teams ("synth/team-00000" style). Tests doing work-order assertions
// use this to construct expected resource names.
func synthTeamRef(team string) string {
	return SynthFDName + "/" + team
}

// smallBOM builds a minimal BOM with explicit fd→teams mapping. Used by
// integration tests that want named teams rather than indexed ones.
func smallBOM(name string, fds map[string][]string) classicbom.ClassicBOMSpec {
	bom := classicbom.ClassicBOMSpec{
		DeploymentInstance: &classicbom.DeploymentInstance{
			Name: name,
		},
	}
	for fdName, teamNames := range fds {
		teams := make([]*classicbom.ServiceTeam, 0, len(teamNames))
		for _, tn := range teamNames {
			teams = append(teams, &classicbom.ServiceTeam{
				Name: tn,
			})
		}
		bom.DeploymentInstance.FunctionalDomains = append(bom.DeploymentInstance.FunctionalDomains, &classicbom.FunctionalDomain{
			Name:              fdName,
			AWSTransitGateway: &classicbom.TransitGW{Enabled: true},
			ServiceTeams:      teams,
		})
	}
	return bom
}

// addTeams appends N synthetic teams to the FD with the given name.
// Returns the slice of new team names added (in append order). Used
// by stress tests to extend an existing spec.
func addTeams(bom *classicbom.ClassicBOMSpec, fdName string, startIdx, count int) []string {
	if bom.DeploymentInstance == nil {
		return nil
	}
	for _, fd := range bom.DeploymentInstance.FunctionalDomains {
		if fd != nil && fd.Name == fdName {
			added := make([]string, 0, count)
			for i := 0; i < count; i++ {
				name := fmt.Sprintf("team-%05d", startIdx+i)
				fd.ServiceTeams = append(fd.ServiceTeams, &classicbom.ServiceTeam{
					Name: name,
				})
				added = append(added, name)
			}
			return added
		}
	}
	return nil
}

// extendBOMEvenly pads every FD's service_teams with synthetic teams so
// the BOM totals at least targetTeams across all FDs. Distribution is
// even — each FD gets approximately targetTeams / len(FDs) more teams.
// The synthetic team names use a "synth-<i>" prefix to avoid colliding
// with real teams.
//
// If the current team count already meets target, the BOM is unchanged.
func extendBOMEvenly(bom *classicbom.ClassicBOMSpec, targetTeams int) {
	if bom.DeploymentInstance == nil || len(bom.DeploymentInstance.FunctionalDomains) == 0 {
		return
	}
	current := 0
	for _, fd := range bom.DeploymentInstance.FunctionalDomains {
		if fd != nil {
			current += len(fd.ServiceTeams)
		}
	}
	if current >= targetTeams {
		return
	}
	deficit := targetTeams - current
	numFDs := 0
	for _, fd := range bom.DeploymentInstance.FunctionalDomains {
		if fd != nil {
			numFDs++
		}
	}
	perFD := deficit / numFDs
	remainder := deficit - perFD*numFDs

	idx := 0
	for _, fd := range bom.DeploymentInstance.FunctionalDomains {
		if fd == nil {
			continue
		}
		toAdd := perFD
		if remainder > 0 {
			toAdd++
			remainder--
		}
		for i := 0; i < toAdd; i++ {
			name := fmt.Sprintf("synth-%07d", idx)
			fd.ServiceTeams = append(fd.ServiceTeams, &classicbom.ServiceTeam{
				Name: name,
			})
			idx++
		}
	}
}
